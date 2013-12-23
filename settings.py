__doc__ = 'default application wide settings'

import sys
import os
import logging


# default location to store output state files
dirname, filename = os.path.split(sys.argv[0])
state_dir = os.path.join(dirname, '.' + filename.replace('.py', '')) 
if not os.path.exists(state_dir):
    try:
        os.mkdir(state_dir)
    except OSError as e:
        state_dir = ''
        #print 'Unable to create state directory:', e
cache_file  = os.path.relpath(os.path.join(state_dir, 'cache.db')) # file to use for pdict cache
queue_file  = os.path.relpath(os.path.join(state_dir, 'queue.db')) # file to use for pdict queue
status_file = os.path.join(state_dir, 'status.js') # where to store state of crawl
log_file    = os.path.join(state_dir, 'webscraping.log') # default logging file

log_level = logging.INFO # logging level
default_encoding = 'utf-8'
default_headers =  {
    'Referer': '', 
    'Accept-Language': 'en-us,en;q=0.5',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
}
