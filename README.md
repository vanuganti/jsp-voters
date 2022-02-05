# AP,JSP voter data extraction,conversion utilities

# To-Do
* Document the process 

# Features
* Connects to [election commission website](http://ceoaperms1.ap.gov.in/Electoral_Rolls/Rolls.aspx), authenticates and pulls the PDF images
* Converts downloaded or supplied PDF files to text
* Converts extracted text file (or supplied one) to voters data as CSV
* Loads the data into mysql database
* Output can also be saved to S3/MySQL database (`--db --s3` arguments)
* Basic validation on what data is missing at district or AC level
* Supports [proxybroker](https://github.com/constverum/ProxyBroker) to use as white-lable IPs for rotation

# Files
* main conversion or parse tool [convert-voters.py](convert-voters.py)
* [export-to-s3.sh](export-to-s3.sh) utility to export csv voter files to s3 
* simple app to show current stats of the voters data

# Website
* simple server to upload files for processing or to download 

# Usage
```
python3 convert-voters.py --help
usage: convert-voters.py [-h] [--debug] [--district DISTRICT] [--ac AC] [--booths BOOTHS] [--threads THREADS] [--dry-run] [--skip-voters] [--skip-proxy] [--enable-lookups] [--text] [--overwrite] [--skip-cleanup] [--stop-on-error] [--limit LIMIT] [--stdout] [--input INPUT] [--csv] [--xls] [--db] [--output OUTPUT] [--s3 S3] [--list-missing] [--metadata]

Parse voters data from image file to CSV

optional arguments:
  -h, --help           show this help message and exit
  --debug              Enable debug mode
  --district DISTRICT  Specific district to be dumped (default None)
  --ac AC              Specific assembly constituency to be dumped (comma separated, default all constituencies)
  --booths BOOTHS      Limit search to the specific booth IDs, separated by comma= (default None)
  --threads THREADS    Max threads (default 1)
  --dry-run            Dry run to test
  --skip-voters        Skip voters data processing (limit to BOOTH details)
  --skip-proxy         Skip proxy to be used for requests
  --enable-lookups     Enable lookups DB with cache (default False)
  --text               Process input text files (default pdf)
  --overwrite          Overwite if file already exists, if not skip processing
  --skip-cleanup       Skip deleting intermediate files post processing
  --stop-on-error      Skip processing upon an error
  --limit LIMIT        Limit total booths (default all booths)
  --stdout             Write output to stdout instead of CSV file
  --input INPUT        Use the input file specified instead of downloading
  --csv                Create CSV file, default False
  --xls                Create XLS file, default False
  --db                 Write to database, default False
  --output OUTPUT      Output folder to store extracted files (default "output")
  --s3 S3              s3 bucket name to store final csv file
  --list-missing       List missing district, AC or booth data
  --metadata           Parse metadata from first page
```
