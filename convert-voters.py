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
import pandas as pd
import hashlib
import redis
from io import BytesIO


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
FAILED_LIST=[]
SUCCESS_LIST=[]
killThreads=False
CSVLOCK=threading.Lock()
REDIS=None

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
    parser.add_argument('--skip-voters', dest='skipvoters', action='store_true', help='Skip voters data processing (limit to BOOTH details)')
    parser.add_argument('--skip-proxy', dest='skipproxy', action='store_true', help='Skip proxy to be used for requests')
    parser.add_argument('--enable-lookups', dest='enable_lookups', default=False, action='store_true', help='Enable lookups DB with cache (default False)')
    parser.add_argument('--limit', dest='limit', type=int, action='store', default=0, help='Limit total booths (default all booths)')
    parser.add_argument('--stdout', dest='stdout', action='store_true', help='Write output to stdout instead of CSV file')
    parser.add_argument('--input', dest='input', type=str, action='store', default=None, help='Use the input file specified instead of downloading')
    parser.add_argument('--csv', dest='csv', action='store_true', default=False, help='Create CSV file, default False')
    parser.add_argument('--xls', dest='xls', action='store_true', default=False, help='Create XLS file, default False')
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
    def __init__(self, session, proxy, url, image):
        self.image = image
        self.session = session
        self.url = url
        self.proxy = proxy

        if self.session and url:
            self.session.headers.update({'referer': self.url})


    #
    # process using using tesseract to get string
    #
    def __image_to_text(self):
        try:
            response = self.session.get("http://ceoaperms.ap.gov.in/Electoral_Rolls/Captcha.aspx", stream=True, proxies=self.proxy)
            if response.status_code != 200:
                logger.error(response)
                return None

            captcha_image = BytesIO()
            for chunk in response:
                captcha_image.write(chunk)

            img = Image.open(captcha_image)
            with BytesIO() as f:
                img.save(f, format="png", quality=600)
                img_png=Image.open(f)
                return pytesseract.image_to_string(img_png, lang='eng', config='-c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 --psm 6', nice=0)

        except KeyboardInterrupt:
            global killThreads
            logger.error("Keyboard interrupt received, killing it")
            killThreads = True
            return None
        except Exception as e:
            logger.error("Failed to parse captcha, " + str(e))
            if "Max retries exceeded" in str(e):
                add_remove_proxy(self.proxy)
            return None

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
                    cell_values.append(int(self.district))
                    cell_values.append(int(self.ac))
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
    'Chrome/54.0.2840.99 Safari/537.36',
    'Chrome/54.0.2840.99 Safari/537.36',
    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/54.0.2840.99 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1)',
    'AppleWebKit/537.36 (KHTML, like Gecko)',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/537.36',
    'Gecko) Chrome',
    'Mozilla/5.0',
    'Windows NT 6.1',
    'Windows NT 10.0; WOW64; rv:50.0'
]


class BoothsDataDownloader:
    def __init__(self, args, district=None, ac=None):
        self.args=args
        self.district=int(district) if district is not None else int(args.district)
        self.ac=int(ac) if ac is not None else int(args.ac) if args.ac is not None else None
        self.proxy={} if args.skipproxy else {'http': choice(PROXY_LIST)}
        self.session=None

    def get_acs(self):
        global killThreads

        try:
            if killThreads:
                return

            self.session = requests.Session()
            self.session.headers.update({'User-Agent': choice(DESKTOP_AGENTS),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'})

            url=None
            if not args.skipproxy:
                logger.info("[%d] Start downloading AC data %s", self.district, self.proxy['http'] if len(self.proxy) > 0 else "")
                for i in range(len(PROXY_LIST)):
                    url = self.__validate_proxy_get_request(self.proxy)
                    if url or len(self.proxy) == 0 or len(PROXY_LIST) == 0:
                        break
                    self.proxy={'http': choice(PROXY_LIST)}
            else:
                logger.info("[%d] Start downloading AC data...", self.district)
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
            result = self.session.post(LOGIN_URL, data=baseData, proxies=self.proxy, timeout=240)
            if not result or result.status_code != 200:
                logger.error("Failed to login, code %d", result.status_code)
                logger.error(result.reason)
                if result and result.status_code >= 500:
                    time.sleep(5)
                return None

            retryCount = 0
            acList = []
            global TOTAL_COUNT

            while retryCount <= 5:
                retryCount+=1

                if not result or not result.text:
                    return None

                soup = BeautifulSoup(result.text, "lxml")
                rows = soup.findAll('select', id='ddlAC')
                for row in rows:
                    acnames=row.findAll('option')
                    for name in acnames:
                        if name['value'] and int(name['value']) == 0:
                            continue
                        acList.append({'value' : name['value'], 'name': name.text.strip()})
                break

            if acList is None or len(acList) == 0:
                logger.error("[%d] FAILED TO PROCESS", self.district)
            return acList

        except KeyboardInterrupt:
            logger.error("Keyboard interrupt received, killing it")
            killThreads = True
            return
        except Exception as e:
            logger.error("[%d] Exception %s", self.district, str(e))
            traceback.print_exc(file=sys.stdout)

    def get_ac_booths(self):
        global killThreads
        try:
            if killThreads:
                return

            self.session = requests.Session()
            self.session.headers.update({'User-Agent': choice(DESKTOP_AGENTS),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'})

            url=None
            if not args.skipproxy:
                logger.info("[%d_%d] Start downloading booth data [%s]", self.district, self.ac, self.proxy['http'] if len(self.proxy) > 0 else "")
                for i in range(len(PROXY_LIST)):
                    url = self.__validate_proxy_get_request(self.proxy)
                    if url or len(self.proxy) == 0 or len(PROXY_LIST) == 0:
                        break
                    self.proxy={'http': choice(PROXY_LIST)}
            else:
                logger.info("[%d_%d] Start downloading booth data...", self.district, self.ac)
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
            result = self.session.post(LOGIN_URL, data=baseData, proxies=self.proxy, timeout=240)
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

                        os.makedirs(os.path.dirname(booth_file), exist_ok=True)

                        col_order=['POLLING STATION NO','POLLING STATION NAME', 'POLLING STATTION LOCATION', 'DISTRICT', 'ASSEMBLY']
                        data_frame=pd.DataFrame(data, columns=col_order)
                        data_frame.to_csv(booth_file, index=False)
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


    def get_booth_voters(self, booth_id):
        if booth_id is None:
            return

        booth_id=int(booth_id)

        try:
            return self.__download_voters_by_booth_id(booth_id)
        except Exception as e:
            logger.error("[%d_%d] Failed to process both voters data for booth ID %d", self.district, self.ac, booth_id)
            logger.error(str(e))
        return None

    def __validate_proxy_get_request(self, random_proxy):
        try:
            return self.session.get(LOGIN_URL, proxies=random_proxy, timeout=15)
        except requests.exceptions.ProxyError as e:
            logger.error("[{}_{}] {}".format(self.district, self.ac, str(e)))
            if len(random_proxy) > 0:
                add_remove_proxy(random_proxy)
        except Exception as e:
            logger.error("[{}_{}] {}".format(self.args.district, self.args.ac, str(e)))
            if len(random_proxy) > 0:
                add_remove_proxy(random_proxy)
        return None

    def __validate_non_proxy_get_request(self):
        try:
            return self.session.get(LOGIN_URL, timeout=30)
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
        results = self.session.post(LOGIN_URL, data=formData, proxies=self.proxy, timeout=240)
        if not results:
            logger.error("[%d_%d] Failed to post for booth data, empty results with proxy %s", self.district,self.ac, self.proxy)
            return None
        if results and results.status_code != 200:
            logger.error("[%d_%d] Failed to post request, code %d", self.district,self.ac, results.status_code)
            logger.error(results.reason)
            return results
        return results


    def __process_captcha_request(self, url, outfile, results, id):

        if not results or not results.text:
            logger.error("[%d_%d_%d] No data from the response, returning", self.district, self.ac, id)
            return add_to_failed_list(id)

        retry_count=0
        html = results.text

        while retry_count <= 20:

            if not html:
                return add_to_failed_list(id)

            logger.debug("[%d_%d_%d] Posting request %s", self.district, self.ac, id, (", retry " + str(retry_count)) if retry_count>0 else "")

            logger.debug("[%d_%d_%d]  Captcha parsing start...", self.district, self.ac, id)
            captcha_text = ImageToText(self.session, self.proxy, url, outfile).get()
            logger.debug("[%d_%d_%d]  Captcha parsing done...", self.district, self.ac, id)

            if not captcha_text:
                return add_to_failed_list(id)

            inputs = BeautifulSoup(html, "lxml").findAll('input')
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
            results = self.session.post(url, data=formData, proxies=self.proxy, timeout=300, stream=True)

            if not results:
                logger.error("[%d_%d_%d] Failed to post, empty results with proxy %s", self.district, self.ac, id, self.proxy)
                return add_to_failed_list(id)

            if results and results.status_code != 200:
                logger.error("[%d_%d_%d] Failed to post request, code %d", self.district, self.ac, id, results.status_code)
                logger.error(results.reason)
                return "ERROR"

            try:
                count=0
                last_chunk=None
                bytes=0

                with open(outfile, 'wb') as myfile:
                    logger.info("[%d_%d_%d]  Downloading the file %s", self.district, self.ac, id, outfile)
                    chunks = results.iter_content(chunk_size=1024*64)
                    for chunk in chunks:
                        last_chunk=chunk
                        count+=1
                        bytes+=len(chunk)
                        myfile.write(chunk)

                if count == 1 and last_chunk is not None:
                    if b'Please enter correct captcha' in last_chunk or b'Enter Verifaction Code' in last_chunk:
                        logger.debug("[%d_%d_%d] Captcha failed %s. retrying %d...", self.district, self.ac, id, captcha_text, retry_count)
                        retry_count+=1
                        html=last_chunk
                        continue

                    if b'error occured on our website' in last_chunk:
                        logger.debug("[%d_%d_%d] Error occured in the page...", self.district, self.ac, id)
                        return "ERROR"

                logger.info("[%d_%d_%d]  File %s downloaded, total bytes: %d", self.district, self.ac, id, outfile, bytes)
                return remove_from_failed_list(id)

            except requests.exceptions.Timeout:
                logger.error("[%d_%d_%d] timeout, retry %d", self.district, self.ac, id)
                return "ERROR"

            except Exception as e:
                logger.error("[%d_%d_%d] Exception, %s", self.district, self.ac, id, str(e))
                return "ERROR"

        return add_to_failed_list(id)


    def __download_voters_by_booth_id(self, id):
        logger.info("[%d_%d_%d] Processing booth %d", self.district, self.ac, id, id)

        if id and id in SUCCESS_LIST:
            logger.info("[%d_%d_%d] Booth already processed, skipping", self.district, self.ac, id, id)
            return remove_from_failed_list(id)

        url="http://ceoaperms.ap.gov.in/Electoral_Rolls/Popuppage.aspx?partNumber="+str(id)+"&roll=EnglishMotherRoll&districtName=DIST_" + str(self.district).zfill(2) + "&acname=AC_" + str(self.ac).zfill(3) + "&acnameeng=A" + str(self.ac).zfill(3) + "&acno=" + str(self.ac) + "&acnameurdu=" + str(self.ac).zfill(3)
        outfile=self.args.output + "/" + str(self.district) + "_" + str(self.ac) + "/" + str(self.district) + "_" + str(self.ac) + "_" + str(id) + ".pdf"
        os.makedirs(os.path.dirname(outfile), exist_ok=True)

        if self.args.dryrun:
            logger.info("[%d_%d_%d] Done Processing booth %d)", self.district, self.ac, id, id)
            return None

        global killThreads

        try:
            retry_count=0

            while retry_count <= 5:
                if killThreads:
                    return None

                if not self.session:
                    self.session = requests.Session()

                self.session.headers.update({'User-Agent': choice(DESKTOP_AGENTS),'Accept':'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'})

                if retry_count > 0:
                    logger.debug("[%d_%d_%d] Retrying the download %d", self.district, self.ac, id, retry_count)

                retry_count+=1

                logger.debug("[%d_%d_%d]  post request without captcha start...", self.district, self.ac, id)
                result = self.session.post(url, proxies=self.proxy, timeout=60)
                logger.debug("[%d_%d_%d]  post request without captcha done...", self.district, self.ac, id)

                if not result:
                    logger.error("[%d_%d_%d] Failed to post for booth data, empty results", self.district,self.ac, id)
                    return add_to_failed_list(id)

                if result and result.status_code != 200:
                    logger.error("[%d_%d_%d] Failed to post request, code %d", self.district,self.ac, id, result.status_code)
                    logger.error(result.reason)
                    continue

                if result and result.status_code == 429:
                    logger.error("[%d_%d_%d] Too many requests warning, sleep & retry %d", self.district,self.ac, id, retry_count)
                    time.sleep(randint(5,10))
                    continue

                data = self.__process_captcha_request(url, outfile, result, id)
                if data and data == "ERROR":
                    logger.error("[%d_%d_%d] Failed to post request, retrying...", self.district, self.ac, id)
                    continue

                return remove_from_failed_list(id)

        except Exception as e:
            logger.error("[%d_%d] Failed to process booth voters data for booth ID %d", self.district, self.ac, id)
            logger.exception("Exception")

        return add_to_failed_list(id)



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
        n=re.sub("\||©|=|=.|\+|\_|\$|—|»", "", str.strip())
        return n.strip()
    return str


class ProcessTextFile():
    def __init__(self, args, input_file):
        self.args=args
        self.input_file=input_file

    def process(self):
        if not self.input_file:
            logger.error("Missing input file, returning")
            return False

        try:
            logger.info("Converting INPUT TEXT FILE %s ", self.input_file)
            file=open(self.input_file, "r")
        except IOError as e:
            logger.exception("Failed to OPEN INPUT FILE %s", self.input_file)
            return False

        metadata={}
        voters=[]
        malformed=[]
        lno=0
        prev_line=None
        voter={}
        booth_name_matched=False
        last_lsn=0
        last_processed_ids=None
        area_name=None
        last_area_name=None
        last_processed_lno=0
        last_match=None
        area_names=[]
        metadata['BOOTH']=""
        metadata['PAGES']=2
        metadata['ASSEMBLY']=""
        assembly_matched=False
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
                        if area_name not in area_names:
                            area_names.append(area_name)
                        metadata['PAGES']+=1
                        continue

                    if metadata['PAGES'] == 2:
                        if "Name and Reservation Status of Parliamentary" in sline:
                            assembly_matched=False
                            try:
                                names=sline.split("  ")
                                a_name=""
                                for name in names[1:]:
                                    n=name.strip()
                                    if n and n!='':
                                        a_name += n + " "
                                metadata['PARLIAMENT']=a_name.replace("  ","").strip()
                            except Exception:
                                metadata['PARLIAMENT']=""
                            continue

                        if "State - Andhra Pradesh" in sline:
                            assembly_matched=True
                            continue

                        if "Name and Reservation Status of" in sline:
                            assembly_matched=False
                            continue

                        if assembly_matched:
                            metadata['ASSEMBLY']+=sline.strip()
                            continue

                        if "Assembly Constituency :" in sline:
                            try:
                                names=sline.split(":")[1].strip().split(" ")
                                for name in names:
                                    n=name.strip()
                                    if n and n != "":
                                        metadata['ASSEMBLY TYPE']=n
                                        break
                            except Exception:
                                metadata['ASSEMBLY TYPE']=""
                            continue

                        if "in which Assembly Constituency" in sline:
                            try:
                                metadata['PARLIAMENT TYPE']=sline.split(":")[1].strip()
                            except Exception:
                                metadata['PARLIAMENT TYPE']=""
                            continue

                        if "Address of Polling Station" in sline:
                            booth_name_matched=True
                            continue

                        if "NUMBER OF ELECTORS" in sline:
                            booth_name_matched=False
                            continue

                        if booth_name_matched:
                            if len(metadata['BOOTH']) >0:
                                metadata['BOOTH']+= "\n"
                            metadata['BOOTH']+=sline.strip()
                            continue

                        if "Main Town " in sline:
                            try:
                                names=re.split("Main Town [:;\.\-\|\>]", re.sub(' +',' ',sline).strip())
                                metadata['MAIN TOWN']=remove_special_chars(names[1].strip())
                            except Exception:
                                metadata['MAIN TOWN']=""
                            continue

                        if "Police Station " in sline:
                            try:
                                names=re.split("Police Station [:;\.\-\|\>]", re.sub(' +',' ',sline).strip())
                                metadata['POLICE STATION']=remove_special_chars(names[1].strip())
                            except Exception:
                                metadata['POLICE STATION']=""
                            continue

                        if "Mandal " in sline:
                            try:
                                names=re.split("Mandal [:;\.\-\|\>]", re.sub(' +',' ',sline).strip())
                                metadata['MANDAL']=remove_special_chars(names[1].strip())
                            except Exception:
                                metadata['MANDAL']=""
                            continue

                        if "District " in sline:
                            try:
                                names=re.split("District [:;\.\-\|\>]", re.sub(' +',' ',sline).strip())
                                metadata['DISTRICT']=remove_special_chars(names[1].strip())
                            except Exception:
                                metadata['DISTRICT']=""
                            continue

                        if "Pin Code " in sline:
                            try:
                                names=re.split("Pin Code ?[:;\.\-\|\>]", re.sub(' +',' ',sline).strip())
                                if len(names) > 1 and names[1] and names[1] != '':
                                    metadata['PINCODE']=names[1].strip()
                                else:
                                    names=re.split("Pin Code ", re.sub(' +',' ',sline).strip())
                                    metadata['PINCODE']=remove_special_chars(names[1].strip())
                            except Exception:
                                metadata['PINCODE']=""
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
                                        voter.setdefault(count,{}).update(SNO=int(found_sno))
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
                                    voter.setdefault(i-1,{}).update(SNO=int(id[0]))
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
                                        voter.setdefault(count,{}).update(SNO=int(sno))
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
                                        pass
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
                                        pass
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
                                        pass
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
                                    age=re.sub("[^0-9]", "", age)
                                    voter.setdefault(count,{}).update(AGE=int(age))
                                except Exception:
                                    age=''
                                    voter.setdefault(count,{}).update(AGE=0)
                                    pass
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
                                    sex=''
                                    voter.setdefault(count,{}).update(SEX=sex)
                                    pass
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

            logger.debug("Malformed records:")
            for x in malformed:
                logger.debug("  {}".format(x))

            metadata['BOOTH']=re.sub("Number of Auxillary Polling|Stations in this Part:|  ","",metadata['BOOTH'].replace("\n",",").strip()).strip()
            if len(voters) > 0:
                try:
                    col_order=['SNO','ID','NAME','FS_NAME','HNO','AGE','SEX','AREA']
                    data_frame=pd.DataFrame(voters, columns=col_order)
                    if args.csv:
                        outfile=os.path.basename(self.input_file).split(".")[0] + ".csv" if self.input_file else "output.csv"
                        if args.output:
                            outfile=args.output + "/" + outfile
                        data_frame.to_csv(outfile, index=False)
                        logger.debug("CSV Output is saved in %s file", outfile)

                    if args.xls:
                        outfile=os.path.basename(self.input_file).split(".")[0] + ".xlsx" if self.input_file else "output.xlsx"
                        if args.output:
                            outfile=args.output + "/" + outfile

                        writer = pd.ExcelWriter(outfile, engine='xlsxwriter')
                        for key,value in data_frame['SEX'].value_counts().iteritems():
                            metadata[key.upper()]=value
                        metadata['TOTAL']=len(voters)
                        details=pd.DataFrame(metadata, index=[0]).T
                        details.to_excel(writer, 'DETAILS')
                        data_frame.to_excel(writer, 'VOTERS DATA', index=False)
                        writer.save()
                        logger.debug("XLS Output is saved in %s file", outfile)

                    if not args.csv and not args.xls:
                        logger.info("No output file supplied, printing to STDOUT")
                        print("\nOUTPUT RECORDS: \n\n")
                        for voter in voters:
                            print(voter)
                        print("\n\n")
                except Exception as e:
                    logger.exception("Exception when writing output")

            logger.info("---------------- S U M M A R Y ----------------------")
            logger.info("Total records: %d, malformed: %d, areas: %d, pages: %d", len(voters), len(malformed), len(area_names), metadata['PAGES'])
            logger.info("{}".format(metadata))
            logger.info("CONVERSION DONE")
            return len(voters) > 0

        except Exception as e:
            logger.error(voter)
            logger.exception("Exception in the line '{}': {}".format(lno, sline))
            return False

def add_to_failed_list(booth_id):
    try:
        if booth_id and booth_id not in FAILED_LIST:
            FAILED_LIST.append(booth_id)
    except Exception as e:
        logger.exception("Failed to add to failed list")
        pass
    return None

def remove_from_failed_list(booth_id):
    try:
        if booth_id and booth_id in FAILED_LIST:
            FAILED_LIST.remove(booth_id)
        if booth_id and booth_id not in SUCCESS_LIST:
            SUCCESS_LIST.append(booth_id)
    except Exception as e:
        logger.exception("Failed from failed list")
        pass
    return None

class DownloadACs:
    def __init__(self, args, district):
        self.args=args
        self.district=district

    def get(self):
        return BoothsDataDownloader(self.args, int(self.district)).get_acs()

class DownloadACBooths:
    def __init__(self, args, district, ac):
        self.args=args
        self.district=district
        self.ac=ac

    def get(self):
        return BoothsDataDownloader(self.args, int(self.district), int(self.ac)).get_ac_booths()

class DownloadVotersByBooth:
    def __init__(self, args, district, ac, id):
        BoothsDataDownloader(args, int(district), int(ac)).get_booth_voters(int(id))

def download_ac_voters_data(args, district, ac, booth_data=None):
    global killThreads

    try:
        if booth_data is None:
            logger.info("[%d_%d] Missing booth data, downloading ...", district, ac)
            data=get_raw_key(str(district) + "_" + str(ac) + "_BOOTHS")
            if data and data is not None:
                logger.info("[%d_%d] Booth data found from cache, %d booths", district, ac, int(data))
                booth_data=range(1, int(data) + 1)
            else:
                data=DownloadACBooths(args, district, ac).get()
                if data is None:
                    logger.error("[%d_%d] Failed to download booth data", district, ac)
                    return None
                set_raw_key(str(district) + "_" + str(ac) + "_BOOTHS", len(data))
                booth_data=range(1, len(data) + 1)

        if args.skipvoters:
            logger.info("VOTERS DATA is skipped due to --skipvoters, total booths: %d", len(booth_data))
            return

        logger.info("Launching %d threads to process %d booths", args.threads, len(booth_data))

        global SUCCESS_LIST, FAILED_LIST
        SUCCESS_LIST=[]
        FAILED_LIST=[]

        booth_output_dir=args.output + "/" + str(district) + "_" + str(ac)
        os.makedirs(booth_output_dir, exist_ok=True)

        count=0
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            for id in booth_data:
                if args.limit > 0 and count >= args.limit:
                    logger.info("[%d_%d] LIMIT %d reached, exiting...", district, ac, args.limit)
                    break
                if killThreads:
                    break
                executor.submit(DownloadVotersByBooth, args, district, ac, id)
                count+=1

            for i in range(2):
                if len(FAILED_LIST) <= 0 or killThreads or args.limit > 0 and count >= args.limit:
                    break
                logger.info("========= PROCESSING FAILED BOOTHS (%d) =============", len(FAILED_LIST))
                with ThreadPoolExecutor(max_workers=args.threads) as executor:
                    for id in FAILED_LIST:
                        if killThreads or args.limit > 0 and count >= args.limit:
                            break
                        executor.submit(DownloadVotersByBooth, args, district, ac, id)
                        count+=1

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt received, killing it")
        killThreads = True
    except Exception as e:
        logger.exception("Exception")

def add_remove_proxy(proxy):
    if proxy and proxy['http'] in PROXY_LIST:
        logger.info("Removing PROXY {}".format(proxy['http']))
        PROXY_LIST.remove(proxy['http'])
        logger.info(PROXY_LIST)
#
# Download booth data
#
def download_booths_data(args, district, ac):
    global killThreads
    global TOTAL_COUNT
    global PROXY_LIST

    district=int(district)

    try:
        if args.skipproxy:
            PROXY_LIST=[]
        else:
            logger.debug("Getting latest PROXY list")
            proxy_list = ProxyList().get(limit=6)
            for proxy in proxy_list:
                try:
                    p = {'http': proxy}
                    result= requests.post("http://ceoaperms.ap.gov.in/Electoral_Rolls/Rolls.aspx", proxies=p, timeout=15)
                    if result.status_code == 200:
                        continue
                    proxy_list.remove(proxy)
                except requests.exceptions.ProxyError as e:
                    logger.exception("Exception, removing {} from proxy list".format(proxy))
                    proxy_list.remove(proxy)
                    continue
                except Exception as e:
                    logger.exception("Exception, removing {} from proxy list".format(proxy))
                    proxy_list.remove(proxy)
                    continue

            if proxy_list and len(proxy_list) > 0:
                logger.debug("Using the proxy list {}".format(proxy_list))
                PROXY_LIST = proxy_list

        if args.booths:
            booth_data=args.booths.split(",")
            logger.info("Using the supplied booths: {}".format(booth_data))
            download_ac_voters_data(args, district, ac, booth_data)
        else:
            if ac is None:
                logger.info("[%d] Missing AC details, fetching AC names", district)
                ac_data=DownloadACs(args, district).get()
                if ac_data is None:
                    logger.error("[%d] Failed to download AC data", district)
                    return
                acs=[ac['value'] for ac in ac_data]
                logger.info("[{}] ACS: {}".format(district, acs))

                for ac in acs:
                    download_ac_voters_data(args, district, int(ac))
            else:
                ac=int(ac)
                logger.info("[%d_%d] Download the booth data", district, ac)
                data=get_raw_key(str(district) + "_" + str(ac) + "_BOOTHS")
                if data is not None:
                    logger.info("[%d_%d] Booth data found from cache, %d booths", district, ac, int(data))
                    booth_data=range(1, int(data) + 1)
                else:
                    data=DownloadACBooths(args, district, ac).get()
                    set_raw_key(str(district) + "_" + str(ac) + "_BOOTHS", len(data))
                    booth_data=range(1, len(data) + 1)
                download_ac_voters_data(args, district, ac, booth_data)

    except KeyboardInterrupt:
        logger.error("Keyboard interrupt received, killing it")
        killThreads = True
    except Exception as e:
        logger.exception("Exception")

def get_md5(filename):
    try:
        hash_md5 = hashlib.md5()
        with open(filename, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        md5=hash_md5.hexdigest()
        logger.info("MD5 for {}: {}".format(filename, md5))
        return md5
    except Exception as e:
        logger.exception("MD5 failed for file %s", filename)
    return None

def get_key(key):
    global REDIS
    if REDIS:
        val=REDIS.get("VOTER-"+key)
        if val:
            return val.decode()
    return None

def set_key(key, value):
    global REDIS
    if REDIS:
        return REDIS.set("VOTER-"+key, value)
    return None

def get_raw_key(key):
    global REDIS
    if REDIS:
        val=REDIS.get("RAW-"+hashlib.md5(key.encode('utf-8')).hexdigest())
        if val:
            return val.decode()
    return None

def set_raw_key(key, value):
    global REDIS
    if REDIS:
        return REDIS.set("RAW-"+hashlib.md5(key.encode('utf-8')).hexdigest(), value)
    return None

class ProcessImageFile():
    def __init__(self, args, input_file):
        self.args=args
        self.input_file=input_file

    def process(self):
        if not os.path.isfile(self.input_file):
            logger.error("Input file " + self.input_file + " does not exists, exiting...")
            sys.exit(1)

        files=os.path.basename(self.input_file).split(".")
        tiff_file=args.output + "/" + os.path.basename(self.input_file).replace(files[len(files)-1],'tiff')
        logger.debug("Converting IMAGE to TEXT ...")
        command="gs -dSAFER -dBATCH -dNOPAUSE -r300 -q -sDEVICE=tiffg4 -sOutputFile='" + tiff_file + "' '" + self.input_file + "'"
        logger.debug(command)
        os.system(command)
        text_file=tiff_file.replace(".tiff", "")
        logger.info("Converting IMAGE to TEXT file (Will take few minutes depending on the size)...")
        command="tesseract '" + tiff_file + "' '" + text_file + "' --psm 6 -l eng -c preserve_interword_spaces=1"
        logger.debug(command)
        os.system(command)
        return ProcessTextFile(args, text_file + ".txt")

#
# process inputfile
#
def process_input_file(input_file, args):
    if input_file.lower().endswith('.txt'):
        logger.info("Input file is TEXT, so skipping image conversion")
        return ProcessTextFile(args, input_file)
    if input_file.lower().endswith('.pdf') or input_file.lower().endswith('.png') or input_file.lower().endswith('.jpeg') or input_file.lower().endswith('.jpg'):
        logger.info("Input file is PDF/IMAGE, doing image conversion")
        return ProcessImageFile(args, input_file)
    if os.path.isdir(input_file):
        logger.info("Input %s is a directory, finding all pdf files for processing", input_file)
        pdf_files=[]
        for root, dirs, files in os.walk(input_file):
            for f in files:
                if f.endswith(".pdf"):
                    pdf_files.append(os.path.join(root, f))
        logger.info("Found %d files in %s, processing using %d threads", len(pdf_files), input_file, args.threads)
        count=0
        with ThreadPoolExecutor(max_workers=args.threads) as executor:
            for file in pdf_files:
                if args.limit > 0 and count >= args.limit:
                    logger.info("LIMIT %d reached, exiting...", args.limit)
                    break
                if killThreads:
                    break
                executor.submit(ProcessImageFile, args, file)
                count+=1
        return
    logger.error("Un-supported input file format, exiting")

#
# process all arguments
#
def handle_arguments(parser, args):
    input_file=None

    if args.output:
        os.makedirs(args.output, exist_ok=True)

    if args.input:
        logger.info("Input file '%s' supplied, using it...", args.input)
        input_file=args.input
        return process_input_file(input_file, args)

    if input_file is None and not args.district:
        logger.error("Missing input file or district/AC details")
        parser.print_usage()
        sys.exit(1)

    district=args.district
    ac=args.ac
    return download_booths_data(args, district, ac)

###################################################################################################
# Main
###################################################################################################
if __name__ == "__main__":
    parser, args = init_options()
    logger.setLevel(logging.DEBUG) if args.debug else logger.setLevel(logging.INFO)

    if args.enable_lookups:
        try:
            REDIS=redis.Redis(host='localhost')
            logger.info("Connected to Redis version %s", REDIS.execute_command('INFO')['redis_version'])
        except Exception as e:
            logger.exception("Failed to connect to Redis, skipping the MD5 lookups")
            REDIS=None
    handle_arguments(parser, args)

