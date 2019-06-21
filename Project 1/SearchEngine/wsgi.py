#!/usr/bin/python3

import logging
import sys
logging.basicConfig(stream=sys.stderr)
sys.path.insert(0, '/home/cs143/www/SearchEngine/SearchEngine/')
from searchengine import app as application

if __name__ == "__main__":
    application.run()
