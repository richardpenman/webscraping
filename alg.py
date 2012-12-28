__doc__ = 'High level functions for interpreting useful data from input'

import os
import re
import csv
import math
import logging
from collections import defaultdict
import common
import xpath


def get_excerpt(html, try_meta=False, max_chars=255):
    """Extract excerpt from this HTML by finding the largest text block

    try_meta: 
        indicates whether to try extracting from meta description tag
    max_chars: 
        the maximum number of characters for the excerpt
    """
    # try extracting meta description tag
    excerpt = ''
    if try_meta:
        excerpt = xpath.get(html, '/html/head/meta[@name="description"]/@content')
    if not excerpt:
        # remove these tags and then find biggest text block
        bad_tags = 'hr', 'br', 'script', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'
        content = common.remove_tags(xpath.get(html, '/html/body', remove=bad_tags))
        if content:
            excerpt = max((len(p.strip()), p) for p in content.splitlines())[1]
    return common.unescape(excerpt.strip())[:max_chars]


def extract_emails(html):
    """Remove common obfuscations from HTML and then extract all emails

    >>> extract_emails('')
    []
    >>> extract_emails('hello contact@webscraping.com world')
    ['contact@webscraping.com']
    >>> extract_emails('hello contact@<!-- trick comment -->webscraping.com world')
    ['contact@webscraping.com']
    >>> extract_emails('hello contact AT webscraping DOT com world')
    ['contact@webscraping.com']
    """
    email_re = re.compile('([\w\.-]{1,64})@(\w[\w\.-]{1,255})\.(\w+)')
    # remove comments, which can obfuscate emails
    html = re.compile('<!--.*?-->', re.DOTALL).sub('', html).replace('mailto:', '')
    emails = []
    for user, domain, ext in email_re.findall(html):
        if ext.lower() not in common.MEDIA_EXTENSIONS and len(ext)>=2 and not re.compile('\d').search(ext) and domain.count('.')<=3:
            email = '%s@%s.%s' % (user, domain, ext)
            if email not in emails:
                emails.append(email)

    # look for obfuscated email
    for user, domain, ext in re.compile('([\w\.-]{1,64})\s?.?AT.?\s?([\w\.-]{1,255})\s?.?DOT.?\s?(\w+)', re.IGNORECASE).findall(html):
        if ext.lower() not in common.MEDIA_EXTENSIONS and len(ext)>=2 and not re.compile('\d').search(ext) and domain.count('.')<=3:
            email = '%s@%s.%s' % (user, domain, ext)
            if email not in emails:
                emails.append(email)
    return emails


def extract_phones(html):
    """Extract phone numbers from this HTML

    >>> extract_phones('Phone: (123) 456-7890 <br>')
    ['(123) 456-7890']
    >>> extract_phones('Phone 123.456.7890 ')
    ['123.456.7890']
    >>> extract_phones('+1-123-456-7890<br />123 456 7890n')
    ['+1-123-456-7890', '123 456 7890']
    """
    phones = []
    for match in re.findall('[\d\-\+ \.\(\)]+', html):
        digits = ''.join([c for c in match if c.isdigit()])
        if len(digits) >= 9:
            # phone should have atleast 9 digits
            phones.append(match.strip())
    return phones


def parse_us_address(address):
    """Parse USA address into address, city, state, and zip code

    >>> parse_us_address('6200 20th Street, Vero Beach, FL 32966')
    ('6200 20th Street', 'Vero Beach', 'FL', '32966')
    """
    city = state = zipcode = ''
    addrs = map(lambda x:x.strip(), address.split(','))
    if addrs:
        m = re.compile('([A-Z]{2,})\s*(\d[\d\-\s]+\d)').search(addrs[-1])
        if m:
            state = m.groups()[0].strip()
            zipcode = m.groups()[1].strip()

            if len(addrs)>=3:
                city = addrs[-2].strip()
                address = ','.join(addrs[:-2])
            else:
                address = ','.join(addrs[:-1])
            
    return address, city, state, zipcode


def distance(p1, p2, scale=None):
    """Calculate distance between 2 (latitude, longitude) points.

    scale:
        By default the distance will be returned as a ratio of the earth's radius
        Use 'km' to return distance in kilometres, 'miles' to return distance in miles

    >>> melbourne = -37.7833, 144.9667
    >>> san_francisco = 37.7750, -122.4183
    >>> int(distance(melbourne, san_francisco, 'km'))
    12659
    """
    lat1, long1 = p1
    lat2, long2 = p2
    # Convert latitude and longitude to 
    # spherical coordinates in radians.
    degrees_to_radians = math.pi / 180.0
        
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

    if scale is None:
        return arc
    elif scale == 'km':
        return arc * 6373
    elif scale == 'miles':
        return arc * 3960
    else:
        raise common.WebScrapingError('Invalid scale: %s' % str(scale))
