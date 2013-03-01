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
default_headers =  {'Accept-encoding': 'gzip', 'Referer': '', 'Accept-Language': 'en-us,en;q=0.5'}
# user-agents for HTTP requests
user_agents = [
    'Mozilla/5.0 (compatible; Baiduspider/2.0; +http://www.baidu.com/search/spider.html)',
    'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)',
    'Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)',
    'Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)',
    'Mozilla/5.0 (Windows; U; Windows NT 6.1; ja; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; ja; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13',
    'Mozilla/5.0 (Windows; U; Windows NT 6.0; ja; rv:1.9.2.13) Gecko/20101203 Firefox/3.6.13 (.NET CLR 3.5.30729)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US) AppleWebKit/534.10 (KHTML, like Gecko) Chrome/8.0.552.215 Safari/534.10',
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; ja; rv:1.9.2.12) Gecko/20101026 Firefox/3.6.12',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.6) Gecko/20060728 Firefox/1.5.0.6',
    'Mozilla/5.0 (X11; Linux i686) AppleWebKit/536.11 (KHTML, like Gecko) Ubuntu/12.04 Chromium/20.0.1132.47 Chrome/20.0.1132.47 Safari/536.11',
    'Opera/9.80 (Windows NT 5.1; U; ja) Presto/2.6.30 Version/10.63'
]
