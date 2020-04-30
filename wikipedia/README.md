# Wikipedia import

This script imports a Wikipedia dump into Elasticsearch. You can download Wikipedia dumps [here](https://dumps.wikimedia.org/enwiki/). Choose the date you're interested in and download a bz2 file (the xml not the much smaller index files). Decompress the bz2 file so you end up with an xml file.

# How to run the script

Clone this repo and install the requirements (they should all be in requirements.txt). Then run the script like this:
```
python3 ingest_elastic.py --host [elastic server name] --port [elastic port] --index [index name] --mapping mapping.json --input [path to xml file]
```

Additional you can specify a user and password used for basic auth (options `--user` and `--password`). If you want to wait every 50 documents to put less pressure on Elasticsearch, you can specify `--wait 1`.
