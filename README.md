# mangrove #
Gathers metadata and transforms it to learning object metadata.

# dependencies #
Dependencies listed for each interface. Currently tested for Python 2.7.

## generic ##
- [lxml](http://lxml.de/)
- [MySQLdb](http://sourceforge.net/projects/mysql-python/)
- [configparser](https://pypi.python.org/pypi/configparser/)
- [prettytable](https://code.google.com/p/prettytable/) for admin

## mediawiki ##
- [NLTK](http://nltk.org/) with stopwords and punkt tokenizer
- [readability-score](http://github.com/wimmuskee/readability-score)
- [wikipedia-extractor](https://github.com/bwbaugh/wikipedia-extractor)
- [requests](http://python-requests.org)

## youtube ##
- [google-api-python-client](http://code.google.com/p/google-api-python-client/)
- [socksipy](http://socksipy.sourceforge.net) for httplib2 proxy

## khanacademy ##
- [requests](http://python-requests.org)
- [RDFLib](https://github.com/RDFLib/rdflib)
- [google-api-python-client](http://code.google.com/p/google-api-python-client/) for purger

# architecture #
Data can be gathered through different supported interfaces. An interface, or *module*, can be configured with a specific *configuration*. For example, multiple Wikipedia instances can be harvested with a single Mediawiki module and different configurations.
The module can be generic, while the config file contains the specific configurations for that module.

# usage #
The mangrove-crawler start harvesting a source by providing:
```bash
./mangrove-crawler.py -s <source>
```

Sources can be defined in the config file as sections. The source is equal to the config section header, and the configuration part needs to correspond to the collection name in the collections database table. Usually, the section header is equal to the collection name.
Here's an example for source "wikipedia_nl" with comments:
```Ini
[common]
# common configuration to be used by all sources
proxy_host=123.123.23.23
proxy_port=3128
# database
db_host=localhost
db_user=root
db_passwd=*******
db_name=mangrove
# filesystem
fs_recordbase=/var/mangrove-records
fs_workbase=/var/mangrove-workdir

[wikipedia_nl]
# the interface module to use (in mangrove_crawler/interfaces)
module=mediawiki
# some module specific vars
wiki=nlwiki
setspec=wikipedia_nl
download_path=http://dumps.wikimedia.org/nlwiki/latest/
host=https://nl.wikipedia.org/wiki/

context_static=PO
context_dynamic=0
age_range=8-12
# whether or not to use the defined proxy
proxy_use=false
```

# roadmap #
The following things to do for a 1.0
- [ ] setup.py
- [ ] debug output optional
- [ ] oaiprovider/uniform database solution for metadata storage
- [ ] fix reporter, for generic data storage
- [ ] command for new source, generating config file, and database entry
- [ ] unittests
