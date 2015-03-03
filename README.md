# mangrove #
Gathers metadata and transforms it to learning object metadata.

# dependencies #
Dependencies listed for each interface. Currently tested for Python 2.7.

## mediawiki ##
- [NLTK](http://nltk.org/) with stopwords and punkt tokenizer
- [readability-score](http://github.com/wimmuskee/readability-score)
- [MySQLdb](http://sourceforge.net/projects/mysql-python/)
- [wikipedia-extractor](https://github.com/bwbaugh/wikipedia-extractor)
- [requests](http://python-requests.org)

## youtube ##
- [google-api-python-client](http://code.google.com/p/google-api-python-client/)
- [pytz](http://pythonhosted.org/pytz/)
- [MySQLdb](http://sourceforge.net/projects/mysql-python/)
- [socksipy](http://socksipy.sourceforge.net) for httplib2 proxy

# usage #
The mangrove-crawler start harvesting a source by providing:
```bash
./mangrove-crawler.py -s <source>
```

Sources can be defined in the config file as sections. Here's an example for source "wikipedia_nl" with comments:
```Ini
[common]
# common configuration to be used by all sources
proxy_host=123.123.23.23
proxy_port=3128

[wikipedia_nl]
# the interface module to use (in mangrove_crawler/interfaces)
module=mediawiki
# some module specific vars
wiki=nlwiki
setspec=wikipedia_nl
download_path=http://dumps.wikimedia.org/nlwiki/latest/
# database
db_host=localhost
db_user=root
db_passwd=*******
db_name=wikilom
# whether or not to use the defined proxy
proxy_use=false
# work here, all working files are here
work_dir=/opt/Wikipedia
```

## roadmap ##
The following things to do for a 1.0
- [ ] setup.py
- [ ] debug output optional
- [ ] oaiprovider
