# This repo is using xbytes API to write data into Elasticsearch

to scrape all platforms, in linux system
# step 0
pip install -r requirements.txt

# step 1
config env
edit INDICES - SCRAPER TEST session
feel free to change v5 to v6 etc.

# step 2
run
```
bash run_scraper.sh
```

# step 3
monitor the log and evaluate the final result in elasticsearch