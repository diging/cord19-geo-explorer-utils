import xml.sax
import json
import getopt
import elasticsearch
import requests
import sys

def create_index(es_object, index_name):
    created = False
    # index settings
    with open('mapping.json') as f:
        settings =  json.load(f)

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name,  body=settings)
            print('[Success] Index created ' + index_name)
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created

def store_record(elastic_object, record, index_name):
    try:
        outcome = elastic_object.index(index=index_name, doc_type='wikientry', body=record)
    except Exception as ex:
        print('[Error] Error in indexing record')
        print(str(ex))

class PageHandler ( xml.sax.ContentHandler):

  def __init__( self, _es, index):
    xml.sax.ContentHandler.__init__( self)
    self._es = _es
    self.index = index
    self.counter = 0
    self.current_element = None
    self.title = None
    self.is_page = False
    self.is_title = False
    self.current_doc = {}
    self.current_text = ""
    self.new_records = True

  def startElement( self, name, attrs):
    if name == "page":
        self.is_page = True
    self.current_element = name
    if name == "title":
        self.is_title = True
    if name == "text" and self.is_page:
        self.counter += 1

  def endElement( self, name):
    if name == "page":
        if self.new_records:
            self.submit_entry()
        else:
            pass
            #print("Skipping " + self.title)
            # let's avoid submitting again
            #if self.title.strip() == "Template:1812 United States elections":
            #    self.new_records = True
        self.is_page = False
        self.current_doc = {}
        self.current_element = None
        self.title = None
        self.current_text = ""
    if name == "title":
        self.is_title = False

  def characters(self, data):
    if self.current_element == "title" and self.is_page and self.is_title:
        self.title = data
        self.current_doc["title"] = data
    if self.current_element == "text" and self.is_page:
        self.current_text = self.current_text + data

  def submit_entry(self):
      lines = []
      self.current_doc["categories"] = []
      for line in self.current_text.splitlines():
          if line.strip().startswith("#REDIRECT"):
              self.current_doc = {}
              break
          if line.strip().startswith("{{short description|"):
              self.current_doc["short_description"] = line.strip()[len("{{short description|"):-2]
          if line.strip().startswith("{{") or line.strip().startswith("|") or line.strip().startswith("*"):
              continue
          if line.strip().startswith("[[Category:"):
              self.current_doc['categories'].append(line.strip()[len("[[Category:"):-2])
          else:
              lines.append(line.strip())
      if self.current_doc:
          self.current_doc["text"] = '\n'.join(lines)
          store_record(self._es, self.current_doc, self.index)
          print("Submitting: " + self.current_doc["title"])
      else:
          print("Redirect: " + self.title)


################ main script ##################

def main(arguments):

    options, remainder = getopt.getopt(arguments, 'i:h:p:', ['input=',
                                                             'host=',
                                                             'port=',
                                                             'index=',
                                                             'user=',
                                                             'mapping=',
                                                             'password=',
                                                             ])

    INDEX_NAME = 'wikipedia_full'
    ELASTIC_HOST = 'localhost'
    ELASTIC_PORT = None
    ES_AUTH_USER = None
    ES_AUTH_PASSWORD = None
    MAPPINGS_JSON = None
    filepath = ''

    for opt, arg in options:
        if opt in ('-i', '--input'):
            filename = arg
        elif opt in ('-h', '--host'):
            ELASTIC_HOST = arg
        elif opt in ('-p', '--port'):
            ELASTIC_PORT = arg
        elif opt in ('--index'):
            INDEX_NAME = arg
        elif opt in ('--mapping'):
            MAPPINGS_JSON = arg
        elif opt in ('--user'):
            ES_AUTH_USER = arg
        elif opt in ('--password'):
            ES_AUTH_PASSWORD = arg

    if not MAPPINGS_JSON:
        print("Please provide a mapping file using the --mapping option.")
        return

    #filename = "/Users/jdamerow/Software/COVID-Explorer/wikipedia/enwiki-20200420-pages-articles-multistream.xml"

    http_auth=()
    if ES_AUTH_USER:
        print("Using authentication for " + ES_AUTH_USER)
        http_auth=ES_AUTH_USER+":"+ES_AUTH_PASSWORD

    print("Connecting to " + ELASTIC_HOST + " on port " + ELASTIC_PORT)
    print("Using index " + INDEX_NAME)
    if ES_AUTH_USER:
        print("Using user " + ES_AUTH_USER)

    if ELASTIC_HOST and ELASTIC_PORT:
        print("Creating elasticsearch")
        es = elasticsearch.Elasticsearch([ELASTIC_HOST], port=ELASTIC_PORT, http_auth=http_auth, connection_class=elasticsearch.RequestsHttpConnection)
    elif ELASTIC_HOST:
        elasticsearch.Elasticsearch([ELASTIC_HOST], http_auth=http_auth, connection_class=elasticsearch.RequestsHttpConnection)
    else:
        elasticsearch.Elasticsearch()

    create_index(es, INDEX_NAME)

    handler = PageHandler(es, INDEX_NAME)

    xml.sax.parse(filename, handler)

####
main(sys.argv[1:])
