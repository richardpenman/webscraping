#
# Description: High level functions for extracting and storing data
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import os
import re
import csv
import math
import logging
from collections import defaultdict
from webscraping import common, xpath


def get_excerpt(html, try_meta=False, max_chars=255):
    """Extract excerpt from this HTML by finding largest text block

    try_meta indicates whether to try extracting from meta description tag
    max_chars is the maximum number of characters for the excerpt
    """
    # try extracting meta description tag
    excerpt = ''
    if try_meta:
        excerpt = xpath.get(html, '/html/head/meta[@name="description"]')
    if not excerpt:
        # remove these tags and then find biggest text block
        bad_tags = 'hr', 'br', 'script', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'
        content = common.remove_tags(xpath.get(html, '/html/body', remove=bad_tags))
        if content:
            excerpt = max((len(p.strip()), p) for p in content.splitlines())[1]
    return common.unescape(excerpt.strip())[:max_chars]


def extract_emails(html):
    """Extract emails and look for common obfuscations

    >>> extract_emails('')
    []
    >>> extract_emails('hello richard@sitescraper.net world')
    ['richard@sitescraper.net']
    >>> extract_emails('hello richard@<!-- trick comment -->sitescraper.net world')
    ['richard@sitescraper.net']
    >>> extract_emails('hello richard AT sitescraper DOT net world')
    ['richard@sitescraper.net']
    """
    email_re = re.compile('[\w\.\+-]{1,64}@\w[\w\.\+-]{1,255}\.\w+')
    # remove comments, which can obfuscate emails
    html = re.compile('<!--.*?-->', re.DOTALL).sub('', html)
    emails = []
    for email in email_re.findall(html):
        if email not in emails:
            emails.append(email)
    # look for obfuscated email
    for user, domain, ext in re.compile('([\w\.\+-]{1,64}) .?AT.? ([\w\.\+-]{1,255}) .?DOT.? (\w+)', re.IGNORECASE).findall(html):
        email = '%s@%s.%s' % (user, domain, ext)
        if email not in emails:
            emails.append(email)
    return emails


def distance(p1, p2):
    """Calculate distance between 2 (latitude, longitude) points
    Multiply result by radius of earth (6373 km, 3960 miles)
    """
    lat1, long1 = p1
    lat2, long2 = p2
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi/180.0
        
    # phi = 90 - latitude
    phi1 = (90.0 - lat1)*degrees_to_radians
    phi2 = (90.0 - lat2)*degrees_to_radians
        
    # theta = longitude
    theta1 = long1*degrees_to_radians
    theta2 = long2*degrees_to_radians
        
    # Compute spherical distance from spherical coordinates.
        
    # For two locations in spherical coordinates 
    # (1, theta, phi) and (1, theta, phi)
    # cosine( arc length ) = 
    #    sin phi sin phi' cos(theta-theta') + cos phi cos phi'
    # distance = rho * arc length
    
    cos = (math.sin(phi1)*math.sin(phi2)*math.cos(theta1 - theta2) + math.cos(phi1)*math.cos(phi2))
    arc = math.acos( cos )

    # Remember to multiply arc by the radius of the earth 
    # in your favorite set of units to get length.
    return arc


def get_logger(output_file, stdout=True, level=logging.DEBUG):
    """Create a logger instance
    """
    logger = logging.getLogger(output_file)
    file_handler = logging.FileHandler(output_file)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    logger.addHandler(file_handler)
    if stdout:
        logger.addHandler(logging.StreamHandler())
    logger.setLevel(level)
    return logger


def read_list(file):
    """Return file as list if exists
    """
    l = []
    if os.path.exists(file):
        l.extend(open(file).read().splitlines())
    else:
        print '%s not found' % file
    return l



class UnicodeWriter(object):
    """A CSV writer that produces Excel-compatibly CSV files from unicode data.
    """
    def __init__(self, filename, encoding='utf-8', mode='w', unique=False):
        self.encoding = encoding
        self.unique = unique
        self.writer = csv.writer(open(filename, mode))
        self.header = None
        self.rows = []
        if unique:# and os.path.exists(filename):
            self.rows = list(csv.reader(open(filename)))

    def cell(self, s):
        if isinstance(s, basestring):
            s = common.unescape(s)
        return s

    def writerow(self, row):
        row = [self.cell(col) for col in row]
        if row not in self.rows:
            self.writer.writerow(row)
            if self.unique:
                self.rows.append(row)

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def writedict(self, d):
        """Write dict to CSV file
        """
        if self.header is None:
            # add header using keys
            # an optional _header attribute defines the column order
            self.header = d.get('_header', sorted(d.keys()))
            self.writerow([col.title() for col in self.header])
        self.writerow([d.get(col) for col in self.header])

    def writedicts(self, rows):
        for d in rows:
            self.writedict(row)
