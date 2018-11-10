#!/usr/bin/python


import logging
import argparse
import os
import requests
import csv
import sys
import re
import time
import traceback
from random import randint, choice
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import threading
import pytesseract
import asyncio
from proxybroker import Broker
try:
    from PIL import Image, ImageEnhance, ImageFilter
except ImportError:
    import Image

logging.basicConfig(level=logging.ERROR, format='%(asctime)s %(lineno)-4d %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("convert-voters")
logger.setLevel(logging.DEBUG)

LOGIN_URL="http://ceoaperms.ap.gov.in/Electoral_Rolls/Rolls.aspx"
TOTAL_COUNT=0
FAILED_KEYWORDS=[]
killThreads=False
CSVLOCK=threading.Lock()

###################################################################################################
# Option handling
###################################################################################################

def init_options():
    parser = argparse.ArgumentParser(description='Parse voters data from image file to CSV')
    parser.add_argument('--debug', dest='debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--district', dest='district', type=int, action='store', default=None, help='Specific district to be dumped (default None)')
    parser.add_argument('--ac', dest='ac', type=int, action='store', default=None, help='Specific assembly constituency to be dumped (default all constituencies)')
    parser.add_argument('--booths', dest='booths', type=str, action='store', default=None, help='Limit search to the specific booth IDs, separated by comma= (default None)')
    parser.add_argument('--threads', dest='threads', type=int, action='store', default=1, help='Max threads (default 1)')
    parser.add_argument('--dry-run', dest='dryrun', action='store_true', help='Dry run to test')
    parser.add_argument('--skip-proxy', dest='skipproxy', action='store_true', help='Skip proxy to be used for requests')
    parser.add_argument('--limit', dest='limit', type=int, action='store', default=0, help='Limit total booths (default all booths)')
    parser.add_argument('--stdout', dest='stdout', action='store_true', help='Write output to stdout instead of CSV file')
    parser.add_argument('--input', dest='input', type=str, action='store', default=None, help='Use the input file specified instead of downloading')
    parser.add_argument('--output', dest='output', type=str, action='store', default='output', help='Output folder to store extracted files (default "output")')
    parser.add_argument('--s3', dest='s3', type=str, action='store', default=None, help='s3 bucket name to store final csv file')
    return parser, parser.parse_args()


###################################################################################################
# Handle arguments
##########s#########################################################################################

##
## district wise assembly
##


##
## DUMP VOTERS
## for AP
# 1-Srikakulam,     1-10
# 2-Vizianagaram,   11-19
# 3-Visakhapatnam,  20-34
# 4-East Godavari,  35-53
# 5-West Godavari,  54-68
# 6-Krishna,        69-84
# 7-Guntur,         85-101
# 8-Prakasam,       102-113
# 9-Nellore,        114-123
# 10-Kadapa,        124-133
# 11-Kurnool,       134-147
# 12-Anantapur,     148-161
# 13-Chittoor,      162-175

class ProxyList:
    async def __append_list(self, proxy_list, proxies):
        while True:
            proxy = await proxies.get()
            if proxy is None: break
            proxy_list.append(proxy.host + ":" + str(proxy.port))

    def get(self, limit=5):
        proxies = asyncio.Queue()
        broker = Broker(proxies)

        proxy_list=[]
        tasks = asyncio.gather(broker.find(types=['HTTP'], post=True, strict=True, limit=limit, countries=['US', 'SG', 'CA']), self.__append_list(proxy_list, proxies))
        loop = asyncio.get_event_loop()
        loop.run_until_complete(tasks)
        return proxy_list

#
# captcha extract from image using teserract
#
class ImageToText:
    def __init__(self, image):
        self.image = image

    #
    # get valid captcha from the image until valid STRING found
    #
    def __get_text_from_image(self):
        for i in range(25):
            image_text = self.__image_to_text()
            if image_text is None:
                return None
            if len(image_text) == 6 and re.match('^[\w-]+$', image_text) is not None:
                return image_text

    #
    # process using using tesseract to get string
    #
    def __image_to_text(self):
        try:
            response = requests.get("http://ceoaperms.ap.gov.in/Electoral_Rolls/Captcha.aspx")
            name=self.image.replace(".pdf", ".gif")
            f = open(name, 'wb')
            f.write(response.content)
            print("3----------------------------------")
            img = Image.open(response.content)
            #img = Image.open(response.content)
            #img = img.convert(quality=100, density=300)
            print("4----------------------------------")
            return pytesseract.image_to_string(img, lang='eng', config='-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 --psm 6', nice=0)
        except KeyboardInterrupt:
            global killThreads
            logger.error("Keyboard interrupt received, killing it")
            killThreads = True
            return None
        except Exception as e:
            logger.error("Failed to parse captcha, " + str(e))
            return None

    #
    # public method
    #
    def get(self, image=None):
        if image != None:
            self.image=image
        return self.__get_text_from_image()

class ParseHtmlTableData:
    def __init__(self, content=None, district=None, ac=None):
        self.content=content
        self.district=district
        self.ac=ac

    def parse(self, content=None):
        if content:
            self.content = content

        tableslist=list()
        if self.content:
            soup = BeautifulSoup(self.content, 'lxml')
            tables = soup.findAll('table', id='GridView1')
            tableslist.append([table for table in tables])
        return self.__store_table(tableslist)

    def __store_table(self, tableslist):
        data=list()
        for table in tableslist:
            if isinstance(table, list):
                for t in table:
                    self.__parse_data_table(data, t)
            else:
                self.__parse_data_table(data, table)
        return data

    def __parse_data_table(self, data, table):
        try:
            rows= table.findAll('tr')
            count=0
            for row in rows:
                cells=row.findAll('td')
                cell_values=[cell.text.strip() for cell in cells]
                if len(cell_values) > 2:
                    count+=1
                    del cell_values[-1]
                    del cell_values[-1]
                    if len(cell_values) != 3:
                        logger.warning("[{}_{}] MALFORMED RECORD {}".format(self.district,self.ac, cell_values))
                        continue
                    data.append(cell_values)

        except KeyboardInterrupt:
            global killThreads
            logger.error("Keyboard interrupt received, killing it")
            killThreads = True
            return None
        except Exception as e:
            logger.error("[%d_%d] Error in processing table data %s", self.district,self.ac, str(e))
            traceback.print_exc(file=sys.stdout)

#
# Do seach (by individual thread with individual session)
#

PROXY_LIST = [
    "http://159.203.73.142:3128",
    "http://165.227.120.233:8080",
    "http://178.32.148.217:80",
    "http://35.236.41.124:80",
    "http://70.184.195.196	:80",
    "http://192.157.252.245:80",
    "http://212.237.52.148:80"
]

DESKTOP_AGENTS = [
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.98 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.71 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'
]

class BoothsDataDownloader:
    def __init__(self, args, district=None, ac=None, id=None):
        self.args=args
        self.district=district if district is not None else args.district
        self.ac=ac if ac is not None else args.ac
        self.proxy={} if args.skipproxy else {'http': choice(PROXY_LIST)}
        self.session=None

        if id is not None:
            try:
                return self.__download_voters_by_booth_id(id)
            except Exception as e:
                logger.error("[%d_%d] Failed to process both voters data for booth ID %d", self.district, self.ac, id)
                logger.error(str(e))
            return None

    def download(self):
        global killThreads
        try:
            if killThreads:
                return

            self.session = requests.Session()
            self.session.headers.update({'User-Agent': choice(DESKTOP_AGENTS),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'})

            url=None
            if not args.skipproxy:
                logger.info("[%d_%d] Start downloading [%s]", self.district, self.ac, self.proxy['http'] if len(self.proxy) > 0 else "")
                for i in range(len(PROXY_LIST)):
                    url = self.__validate_proxy_get_request(self.proxy)
                    if url or len(self.proxy) == 0 or len(PROXY_LIST) == 0:
                        break
                    self.proxy={'http': choice(PROXY_LIST)}
            else:
                logger.info("[%d_%d] Start downloading ...", self.district, self.ac)
                url = self.__validate_non_proxy_get_request()

            if url is None:
                return None
            soup = BeautifulSoup(url.text, "lxml")
            view_state = ''
            event_validation = ''

            inputs = soup.findAll('input')
            for input in inputs:
                if input['name'] == '__EVENTVALIDATION':
                    event_validation = input['value']
                if input['name'] == '__VIEWSTATE':
                    view_state = input['value']

            baseData = {
                  '__EVENTTARGET' : 'ddlDist',
                  '__EVENTARGUMENT' : '',
                  '__LASTFOCUS' : '',
                  '__VIEWSTATE' : view_state,
                  '__EVENTVALIDATION' : event_validation,
                  'ddlDist': self.district,
                  'ddlAC': 0
            }
            result = self.session.post(LOGIN_URL, data=baseData, proxies=self.proxy)
            if not result or result.status_code != 200:
                logger.error("Failed to login, code %d", result.status_code)
                logger.error(result.reason)
                if result and result.status_code >= 500:
                    time.sleep(5)
                return None

            retryCount = 0
            boothsList = None
            global TOTAL_COUNT

            while retryCount <= 5:
                retryCount+=1
                result = self.__post_request(result)
                if not result:
                    break
                if result and result.status_code >= 500:
                    continue
                if result.status_code == 200:
                    if "An unexpected error occured" in result.text:
                        break

                    data = ParseHtmlTableData(result.text, self.district, self.ac).parse()
                    if data and len(data) > 0:
                        boothsList=data
                        logger.info("[%d_%d] Found %d booths", self.district, self.ac, len(data))
                        booth_file=args.output + "/" + str(self.district) + "_" + str(self.ac) + "/" +str( self.district) + "_" + str(self.ac) + "_booths.csv"
                        with open(booth_file, 'w') as myfile:
                            wr=csv.writer(myfile, quoting=csv.QUOTE_ALL)
                            wr.writerow(['POLLING STATION NO','POLLING STATION NAME', 'POLLING STATTION LOCATION'])
                            wr.writerows(data)
                        break

                else:
                    logger.error("[%d_%d] Failed to fetch content %d", self.district, self.ac, result.status_code)
                    logger.debug(result.text)
            if boothsList is None:
                logger.error("[%d_%d] FAILED TO PROCESS", self.district, self.ac)
            return boothsList
        except KeyboardInterrupt:
            logger.error("Keyboard interrupt received, killing it")
            killThreads = True
            return
        except Exception as e:
            logger.error("[%d_%d] Exception %s", self.district, self.ac, str(e))
            traceback.print_exc(file=sys.stdout)

    def __validate_proxy_get_request(self, random_proxy):
        try:
            return self.session.get(LOGIN_URL, proxies=random_proxy, timeout=30)
        except requests.exceptions.ProxyError as e:
            logger.error("[{}_{}] {}".format(self.district, self.ac, str(e)))
            if len(random_proxy) > 0:
                logger.info("Removing proxy %s", random_proxy['http'])
                if random_proxy['http'] in PROXY_LIST:
                    PROXY_LIST.remove(random_proxy['http'])
                    logger.info(PROXY_LIST)
        except Exception as e:
            logger.error("[{}_{}] {}".format(self.args.district, self.args.ac, str(e)))
            if len(random_proxy) > 0:
                logger.info("Removing proxy %s", random_proxy['http'])
                if random_proxy['http'] in PROXY_LIST:
                    PROXY_LIST.remove(random_proxy['http'])
                    logger.info(PROXY_LIST)
        return None

    def __validate_non_proxy_get_request(self):
        try:
            return self.session.get(LOGIN_URL, timeout=10)
        except Exception as e:
            logger.error("[{}_{}] {}".format(self.args.district, self.args.ac, str(e)))
        return None

    #
    # post request for named search
    #
    def __post_request(self, result):
        if not result or not result.text:
            return None

        soup = BeautifulSoup(result.text, "lxml")
        inputs = soup.findAll('input')
        view_state = ''
        event_validation = ''
        for input in inputs:
            if input['name'] == '__EVENTVALIDATION':
                event_validation = input['value']
            if input['name'] == '__VIEWSTATE':
                view_state = input['value']

        formData = {
          '__EVENTTARGET' : '',
          '__EVENTARGUMENT' : '',
          '__LASTFOCUS' : '',
          '__VIEWSTATE' : view_state,
          '__EVENTVALIDATION' : event_validation,
          'ddlDist': self.district,
          'ddlAC': self.ac,
          'btnGetPollingStations': 'Get Polling Stations'
        }

        time.sleep(randint(2,6)) # to fake as human
        results = self.session.post(LOGIN_URL, data=formData, proxies=self.proxy, timeout=60)
        if not results:
            logger.error("[%d_%d] Failed to post for booth data, empty results with proxy %s", self.district,self.ac, self.proxy)
            return None
        if results and results.status_code != 200:
            logger.error("[%d_%d] Failed to post request, code %d", self.district,self.ac, results.status_code)
            logger.error(results.reason)
            return results
        return results


    def __download_voters_by_booth_id(self, id):
        logger.info("[%d_%d_%d] Processing booth %d", self.district, self.ac, id, id)
        url="http://ceoaperms.ap.gov.in/Electoral_Rolls/Popuppage.aspx?partNumber=1&roll=EnglishMotherRoll&districtName=DIST_" + str(self.district).zfill(2) + "&acname=AC_" + str(self.ac).zfill(3) + "&acnameeng=A" + str(self.ac).zfill(3) + "&acno=" + str(id) + "&acnameurdu=" + str(id)
        retryCount=0
        outfile=self.args.output + "/" + str(self.district) + "_" + str(self.ac) + "/" + str(self.district) + "_" + str(self.ac) + "_" + str(id) + "_1.pdf"

        global killThreads
        try:
            if killThreads:
                return None

            while retryCount <= 25:
                session = requests.Session()
                session.headers.update({'User-Agent': choice(DESKTOP_AGENTS),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'})

                retryCount+=1
                logger.debug("Retrying the download %d", retryCount)
                result = session.post(url, proxies=self.proxy, timeout=60)
                if not result:
                    logger.error("[%d_%d_%d] Failed to post for booth data, empty results", self.district,self.ac, id)
                    FAILED_KEYWORDS.append(id) if id not in FAILED_KEYWORDS else {}
                    return None
                if result and result.status_code != 200:
                    logger.error("[%d_%d_%d] Failed to post request, code %d", self.district,self.ac, id, result.status_code)
                    logger.error(result.reason)
                    continue

                soup = BeautifulSoup(result.text, "lxml")
                imgs = soup.findAll('img')  # Perform login

                captcha_text = None
                for img in imgs:
                    if 'Captcha' in img['src']:
                        captcha_text = ImageToText(outfile).get()
                        logger.info("[%d_%d] Captcha text: %s", self.district, self.ac, captcha_text)

                inputs = soup.findAll('input')
                view_state = ''
                event_validation = ''
                for input in inputs:
                    if input['name'] == '__EVENTVALIDATION':
                        event_validation = input['value']
                    if input['name'] == '__VIEWSTATE':
                        view_state = input['value']

                formData = {
                    '__VIEWSTATE' : view_state,
                    '__EVENTVALIDATION' : event_validation,
                    'txtVerificationCode': captcha_text,
                    'btnSubmit': 'Submit'
                }

                time.sleep(randint(2,6)) # to fake as human
                results = session.post(url, data=formData, proxies=self.proxy, timeout=240, stream=True)
                if not results:
                    logger.error("[%d_%d_%d] Failed to post, empty results with proxy %s", self.district, self.ac, id, self.proxy)
                    FAILED_KEYWORDS.append(id) if id not in FAILED_KEYWORDS else {}
                    return None
                if results and results.status_code != 200:
                    logger.error("[%d_%d_%d] Failed to post request, code %d", self.district, self.ac, id, results.status_code)
                    logger.error(results.reason)
                    continue

                with open(outfile, 'wb') as myfile:
                    #for block in results.iter_content(chunk_size=1024):
                    myfile.write(results.content)
                    logger.info("[%d_%d_%d] Successfully downloaded the file", self.district, self.ac, id )
                return results
        except Exception as e:
            logger.error("[%d_%d] Failed to process both voters data for booth ID %d", self.district, self.ac, id)
            logger.error(str(e))


def get_id_between(line, start, end, prefix):
    cond=prefix + str(start) + " | " + str(end) + " "
    ids=re.split(cond, line)
    logger.debug("Split cond: {}:{} => {} ".format(start, end, ids))
    if len(ids) == 1:
        logger.debug(" return {}:{}".format(str(start), ids[0].strip().replace(" ","")))
        return [str(start), ids[0].strip().replace(" ",""), False]
    if str(start) + " " not in line:
        logger.debug(" return {}:{}:MALFORMED".format(str(start), ids[0].strip().replace(" ","")))
        return [str(start), ids[0].strip().replace(" ",""), True]
    logger.debug(" return {}:{}".format(str(start), ids[1].strip().replace(" ","")))
    return [str(start), ids[1].strip().replace(" ",""), False]

def remove_special_chars(str):
    if str and len(str) > 0:
        n=re.sub("\||©|=|=.|\+|\_|\$|—", "", str.strip())
        return n.strip()
    return str

def parse_voters_data(args, input_file):
    if not input_file:
        logger.error("Missing input file, returning")
        return

    try:
        logger.info("Converting INPUT TEXT FILE %s into CSV file", input_file)
        file=open(input_file, "r")
    except IOError as e:
        logger.error("Failed to OPEN INPUT FILE %s", input_file)
        logger.error(str(e))
        return

    voters=[]
    malformed=[]
    lno=0
    prev_line=None
    voter={}
    booth_name_matched=False
    booth_name=None
    last_lsn=0
    last_processed_ids=None
    area_name=None
    last_area_name=None
    last_processed_lno=0
    last_match=None
    try:
        for line in file:
            lno+=1
            sline=line.strip()
            if sline and len(sline) > 0:
                if "Contd..." in sline:
                    last_area_name=area_name
                    area_name=sline.replace("Contd...","").strip()
                    if last_area_name is None:
                        last_area_name=area_name
                    continue

                if "Address of Polling Station" in sline:
                    booth_name_matched=True
                    last_match='BOOTH'
                    continue

                if booth_name_matched:
                    booth_name=sline.replace('Number of Auxillary Polling','').strip()
                    logger.info("Found booth name: %s", booth_name)
                    booth_name_matched=False
                    continue

                if "Elector's Name" in sline or "Elector Name" in sline or "Electors Name" in sline or "Elector’s Name" in sline:
                    if len(voter) > 0:
                        for v in voter:
                            data=voter[v]
                            if len(data)  != 7:
                                logger.error("ERROR records found at line {} for IDS {}".format(last_processed_lno-1, last_processed_ids))
                                logger.error("ERROR RECORD: {}".format(data))
                                logger.info(voter)
                                logger.info("CURRENT LINE {} : {}, PREVIOUS LINE ".format(lno, sline, prev_line))
                                return
                            try:
                                lsn=int(voter[v][0]['SNO'])
                                if lsn > last_lsn:
                                    last_lsn=lsn
                            except Exception as e:
                                last_lsn+=1
                                pass
                            data.update({"AREA": last_area_name})
                            voters.append(data)
                        voter={}
                    last_match='NAME'
                    names=re.split("Elector’s Name:|Elector Name[:;]|Electors Name[:;]|Elector's Name[:;]|Elector’s Name[:;]", sline)
                    if "       " in prev_line:
                        ids=prev_line.split("       ")
                        logger.debug("IDS with spaces {}".format(ids))
                        count=0
                        found_sno=None
                        found_id=None
                        for i in range(0, len(ids)):
                            id=ids[i].strip()
                            if len(id) > 0:
                                if id.isnumeric():
                                    logger.debug("Found SNO %s at %d", id, i)
                                    found_sno=id
                                    continue
                                else:
                                    if found_sno:
                                        logger.debug("Found ID %s at %d", id, i)
                                        found_id=id.replace(" ","")
                                    else:
                                        logger.debug("MISSING ID FOUND %s at %d, matched %d", id, i, last_lsn+count+1)
                                        found_sno=last_lsn+count+1
                                if found_sno and found_id:
                                    logger.debug("Assing ids: {}:{}".format(found_sno, found_id))
                                    voter.setdefault(count,{}).update(SNO=found_sno)
                                    voter.setdefault(count,{}).update(ID=found_id)
                                    count+=1
                                    found_sno=None
                                    found_id=None
                    else:
                        ids=prev_line.split(" ")
                        if len(ids) > 6:
                            logger.debug("IDs length mismatch {}, {}".format(len(ids), ids))
                            for i in range(1,4):
                                id=get_id_between(prev_line, last_lsn+i, last_lsn+i+1, "" if i == 1 else " ")
                                voter.setdefault(i-1,{}).update(SNO=id[0])
                                voter.setdefault(i-1,{}).update(ID=id[1])
                                if id[2] is True:
                                   logger.warning("Malformed record found for sequence {} at line {} ({})".format(last_lsn+i, lno, prev_line))
                                   malformed.append({ "LINE " + str(lno).rjust(4) : "For Sequence " + str(last_lsn+i).rjust(4) + " => " + prev_line})
                        else:
                            count=0
                            sno=None
                            iname=None
                            for id in ids:
                                id=id.strip()
                                if id and id != '':
                                    if not sno:
                                        if not id.isnumeric():
                                            logger.warning("SNO is not numeric({}), assigning auto-increment value({}) at line {}".format(id, last_lsn+count+1, lno))
                                            sno=str(last_lsn+count+1)
                                            continue
                                        sno=id
                                        continue
                                    if id == 'APO':
                                        if not iname:
                                            iname=id
                                        continue
                                    if iname:
                                        id=str(iname+""+id)
                                    voter.setdefault(count,{}).update(SNO=sno)
                                    voter.setdefault(count,{}).update(ID=id)
                                    sno=None
                                    iname=None
                                    count+=1

                    count=0
                    last_processed_ids=prev_line
                    last_processed_lno=lno
                    for name in names:
                        n=remove_special_chars(name)
                        if n and n != '':
                            voter.setdefault(count,{}).update(NAME=n)
                            count+=1
                    if count < len(voter):
                        logger.debug("Problem with matching the NAMES (found %d records for %d) for line %s, manually parsing", count, len(voter), sline)
                        names=re.split("                               ", sline)
                        count=0
                        for name in names:
                            n=name.strip()
                            if n and n != '':
                                logger.debug(re.split(":|;", n))
                                try:
                                    nn=re.split(":|;", n)
                                    voter.setdefault(count,{}).update(NAME=nn[1].strip())
                                except Exception:
                                    voter.setdefault(count,{}).update(NAME=nn[0].strip())
                                logger.debug(voter[count]["FS_NAME"])
                                count+=1
                    continue
                if "Husband's Name" in sline or "Father's Name" in sline or "Husband" in sline or "Father" in sline or "Mother's Name" in sline or "Mother" in sline or "Other's Name" in sline or "Others Name" in sline or "Other Name" in sline:
                    last_match='FS_NAME'
                    names=re.split("Husband's Name[:;]|Husband Name[:;]|Husbands Name[:;]|Father's Name[:;]|Father Name[:;]|Fathers Name[:;]|Mothers Name[:;]|Mother Name[:;]|Mother's Name[:;]|Mother’s Name[:;]|Father's Name[;:]|Others Name[:;]|Other Name[:;]|Other's Name[:;]", sline)
                    count=0
                    logger.debug(names)
                    for name in names:
                        n=remove_special_chars(name)
                        if n and n != '':
                            voter.setdefault(count,{}).update(FS_NAME=n)
                            count+=1
                    if count < len(voter):
                        logger.debug("Problem with matching the FNAMES (found %d records for %d) for line %s, manually parsing", count, len(voter), sline)
                        names=re.split("                               ", sline)
                        count=0
                        for name in names:
                            n=name.strip()
                            if n and n != '':
                                logger.debug(re.split(":|;", n))
                                try:
                                    nn=re.split(":|;", n)
                                    voter.setdefault(count,{}).update(FS_NAME=nn[1].strip())
                                except Exception:
                                    voter.setdefault(count,{}).update(FS_NAME=nn[0].strip())
                                logger.debug(voter[count]["FS_NAME"])
                                count+=1

                    continue
                if "House No" in sline or "House" in sline:
                    last_match='HNO'
                    names=re.split("House No[:;]", sline)
                    count=0
                    logger.debug(names)
                    for name in names:
                        n=name.strip()
                        logger.debug(n)
                        if n and n != '':
                            voter.setdefault(count,{}).update(HNO=n)
                            count+=1
                    if count < len(voter):
                        logger.debug("Problem with matching the HNO (found %d records for %d) for line %s, manually parsing", count, len(voter), sline)
                        names=re.split("  ", sline)
                        count=0
                        for name in names:
                            n=name.strip()
                            if n and 'House' in name:
                                try:
                                    nn=re.split(":|;", n)
                                    voter.setdefault(count,{}).update(HNO=nn[1].strip())
                                except Exception:
                                    voter.setdefault(count,{}).update(HNO='')
                                logger.debug(voter[count]["HNO"])
                                count+=1
                    continue
                if "Age" in sline and "Sex" in sline:
                    names=sline.split(" ")
                    logger.debug(names)
                    l=len(names)
                    count=0
                    age=None
                    sex=None
                    for index, obj in enumerate(names):
                        obj=obj.strip()
                        if "Age" in obj:
                            try:
                                c=index+1
                                age=""
                                while True:
                                    age=names[c].strip()
                                    if age and age != '':
                                        break
                                    c+=1
                                voter.setdefault(count,{}).update(AGE=age)
                            except Exception:
                                logger.error("Exception age")
                                age=''
                                voter.setdefault(count,{}).update(AGE='')
                        elif "Sex" in obj:
                            try:
                                c=index+1
                                sex=""
                                while True:
                                    sex=names[c].strip()
                                    if sex and sex != '':
                                        break
                                    c+=1
                                voter.setdefault(count,{}).update(SEX=sex)
                            except Exception:
                                logger.error("Exception sex")
                                sex=''
                                voter.setdefault(count,{}).update(SEX=sex)
                        if age and sex:
                            logger.debug(voter[count])
                            count+=1
                            age=None
                            sex=None
                    continue
                if last_match == 'NAME' or last_match == 'FS_NAME':
                    names=sline.split("                      ")
                    logger.debug("Last matched name {}, ids: {}".format(last_match, names))
                    count=0
                    for name in names:
                        n=name.strip()
                        if n and n != '':
                            v_name=voter[count][last_match]
                            v_name += " " + remove_special_chars(n)
                            voter[count][last_match]=v_name
                            count+=1
                prev_line=sline

        if len(voter) != 0:
            for v in voter:
                data=voter[v]
                data.update({"AREA": last_area_name})
                voters.append(data)

        logger.info("---------------- S U M M A R Y ----------------------")
        if len(voters) > 0:
            outfile=os.path.basename(input_file).split(".")[0] + ".csv" if input_file else "output.csv"
            if args.output:
                outfile=args.output + "/" + outfile
            with open(outfile, 'w') as myfile:
                order=['SNO','ID','NAME','FS_NAME','HNO','AGE','SEX','AREA']
                fp = csv.DictWriter(myfile, order, quoting=csv.QUOTE_ALL)
                fp.writeheader()
                fp.writerows(voters)
            logger.debug("Output is saved in %s file", outfile)

        logger.info("Total records: %d, malformed: %d", len(voters), len(malformed))
        logger.debug("Malformed records:")
        for x in malformed:
            logger.debug("  {}".format(x))
        logger.debug("-----------------------------------------------------")

    except Exception as e:
        logger.error("Exception in the line '{}': {}".format(lno, sline))
        logger.error(voter)
        traceback.print_exc(file=sys.stdout)


#
# Download booth data
#
def download_booths_data(args, district, ac):
    global killThreads
    global TOTAL_COUNT
    global PROXY_LIST

    try:
        if args.skipproxy:
            PROXY_LIST=[]
        else:
            logger.debug("Getting latest PROXY list")
            proxy_list = ProxyList().get(limit=5)
            for proxy in proxy_list:
                try:
                    p = {'http': proxy}
                    result= requests.post("http://ceoaperms.ap.gov.in/Electoral_Rolls/Rolls.aspx", proxies=p, timeout=15)
                    if result.status_code == 200:
                        continue
                    proxy_list.remove(proxy)
                except requests.exceptions.ProxyError as e:
                    logger.error("Exception, removing {} from proxy list {}".format(proxy, str(e)))
                    proxy_list.remove(proxy)
                    continue
                except Exception as e:
                    logger.error("Exception, removing {} from proxy list {}".format(proxy, str(e)))
                    proxy_list.remove(proxy)
                    continue

            if proxy_list and len(proxy_list) > 0:
                logger.debug("Using the proxy list {}".format(proxy_list))
                PROXY_LIST = proxy_list

        if args.booths:
            booth_data=args.booths.split(",")
            logger.info("Using the supplied booths: {}".format(booth_data))
        else:
            logger.info("Download the booth data for district %d, ac: %d", district, ac)
            booth_output_dir=args.output + "/" + str(district) + "_" + str(ac)
            os.makedirs(booth_output_dir, exist_ok=True)
            booth_data=BoothsDataDownloader(args, district, ac).download()

        logger.info("Launching %d threads to process %d booths", args.threads, len(booth_data))

        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            for id in range(0, len(booth_data)):
                logger.info("Thread %d", id)
                if args.limit > 0 and id >= args.limit:
                    logger.info("[%d_%d] LIMIT %d reached, exiting...", district, ac, args.limit)
                    break
                if killThreads:
                    break
                executor.submit(BoothsDataDownloader, args, district, ac, id+1)

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt received, killing it")
        killThreads = True
    except Exception as e:
        logger.error(str(e))
        traceback.print_exc(file=sys.stdout)

#
# convert image to text
#
def convert_image_file_to_text(args, input_file):
    if not os.path.isfile(input_file):
        logger.error("Input file " + input_file + " does not exists, exiting...")
        sys.exit(1)

    files=os.path.basename(input_file).split(".")
    tiff_file=args.output + "/" + os.path.basename(input_file).replace(files[len(files)-1],'tiff')
    logger.debug("Converting IMAGE to TEXT ...")
    command="gs -dSAFER -dBATCH -dNOPAUSE -r300 -q -sDEVICE=tiffg4 -sOutputFile='" + tiff_file + "' '" + input_file + "'"
    logger.debug(command)
    os.system(command)
    text_file=tiff_file.replace(".tiff", "")
    logger.info("Converting IMAGE to TEXT file (Will take few minutes depending on the size)...")
    command="tesseract '" + tiff_file + "' '" + text_file + "' --psm 6 -l eng -c preserve_interword_spaces=1"
    logger.debug(command)
    os.system(command)
    return parse_voters_data(args, text_file + ".txt")


#
# process inputfile
#
def process_input_file(input_file, args):
    if input_file.lower().endswith('.txt'):
        logger.info("Input file is TEXT, so skipping image conversion")
        return parse_voters_data(args, input_file)
    if input_file.lower().endswith('.pdf') or input_file.lower().endswith('.png') or input_file.lower().endswith('.jpeg') or input_file.lower().endswith('.jpg'):
        logger.info("Input file is PDF/IMAGE, doing image conversion")
        return convert_image_file_to_text(args, input_file)
    logger.error("Un-supported input file format, exiting")


def test():
    last_lsn = 27
    count = 0
    prev_line = "1 UON 1884386 2 UON1886282 3 UON1 906486"
    prev_line = "28 AP07048036035 1 29 UON1998699 30 AP070480054723"
    prev_line = "25          UON1799347                                26          UON1806613                                27          UON1542274"
    logger.info("LINE {}".format(prev_line))
    if "          " in prev_line:
        ids=prev_line.split("          ")
        for i in range(1, len(ids)):
            id=ids[i-1].strip()
            if id.isnumeric():
                logger.info("SNO: " + id)
                logger.info("ID: " + ids[1].replace(" ","").strip())
    else:
        if "|" in prev_line:
            ids = prev_line.split(" | ")
        else:
            ids = prev_line.split("          ")
        logger.info("SPLIT LENGTH: {},  {}".format(len(ids), ids))
        if len(ids) > 6:
            logger.info("IDs length mismatch {}, {}".format(len(ids), ids))
            for i in range(1, len(ids)):
                id=ids[i-1].strip()
                if id.isnumeric():
                    logger.info("SNO: " + id)
                    logger.info("ID: " + ids[1].replace(" ","").strip())
            for i in range(1, 4):
                id = get_id_between(prev_line, last_lsn + i, last_lsn + i + 1, "" if i == 1 else "          ")
                logger.info("SNO: " + id[0])
                logger.info("ID: " + id[1])
                if id[2] is True:
                    logger.warning(
                        "Malformed record found for sequence {} at line {} ({})".format(last_lsn + i, 1, prev_line))
        else:
            count = 0
            sno = None
            iname = None
            for id in ids:
                id = id.strip()
                if id and id != '':
                    if not sno:
                        if not id.isnumeric():
                            logger.warning(
                                "SNO is not numeric({}), assigning auto-increment value({}) at line {}".format(id,
                                                                                                               last_lsn + count + 1,
                                                                                                               lno))
                            sno = str(last_lsn + count + 1)
                            continue
                        sno = id
                        continue
                    if id == 'APO':
                        if not iname:
                            iname = id
                        continue
                    if iname:
                        id = str(iname + "" + id)
                    logger.info("SNO: " + sno)
                    logger.info("ID: " + id)
                    sno = None
                    iname = None
                    count += 1
#
# process all arguments
#
def handle_arguments(parser, args):
    input_file=None

    if args.output:
        os.makedirs(args.output, exist_ok=True)

    start=time.time()
    if args.input:
        logger.info("Input file '%s' supplied, using it...", args.input)
        input_file=args.input
        output=process_input_file(input_file, args)
        logger.info("TOTAL EXECUTION TIME: %d secs", time.time() - start)
        return output

    if input_file is None and not args.district or not args.ac:
        logger.error("Missing input file or district/AC details")
        parser.print_usage()
        sys.exit(1)

    district=args.district
    ac=args.ac

    output=download_booths_data(args, district, ac)
    logger.info("TOTAL EXECUTION TIME: %d secs", time.time() - start)
    return output

###################################################################################################
# Main
###################################################################################################
if __name__ == "__main__":
    parser, args = init_options()
    logger.setLevel(logging.DEBUG) if args.debug else logger.setLevel(logging.INFO)
    #test()
    handle_arguments(parser, args)

