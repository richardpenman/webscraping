#
# Description: High level functions for extracting and storing data
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import csv
import math
from webscraping import common, xpath


def get_excerpt(html, try_meta=False, max_chars=255):
    """Extract excerpt from this HTML by finding largest text block

    try_meta indicates whether to try extracting from meta description tag
    max_chars is the maximum number of characters for the excerpt
    """
    # try extracting meta description tag
    excerpt = xpath.get(html, '/html/head/meta[@name="description"]') if try_meta else ''
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


class UnicodeWriter(object):
    """A CSV writer that produces Excel-compatibly CSV files from unicode data.
    """
    def __init__(self, filename, encoding='utf-8'):
        self.writer = csv.writer(open(filename, 'w'))
        self.encoding = encoding

    def cell(self, s):
        if isinstance(s, basestring):
            try:
                s = s.decode(self.encoding, 'ignore')
            except UnicodeError:
                pass
            try:
                s = s.encode(self.encoding, 'ignore')
            except UnicodeError:
                pass
        return s

    def writerow(self, row):
        self.writer.writerow([self.cell(col) for col in row])

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

    def writedicts(self, rows):
        """Write dict to CSV file
        """
        header = None
        for d in rows:
            if header is None:
                header = sorted(d.keys())
                self.writerow([col.title() for col in header])
            self.writerow([d[col] for col in header])
