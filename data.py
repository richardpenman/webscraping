#
# Description: High level functions for extracting and storing data
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import csv
from webscraping import common, xpath


def get_excerpt(html, try_meta=True, max_chars=255):
    """Extract excerpt from this HTML by finding largest text block

    try_meta indicates whether to try extracting from meta description tag
    max_chars is the maximum number of characters for the excerpt
    """
    excerpt = xpath.get(html, '/html/head/meta[@name="description"]') if try_meta else None
    if not excerpt:
        content = common.remove_tags(xpath.get(html, '/html/body', remove=['hr', 'br', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']))
        excerpt = max((len(p.strip()), p) for p in content.splitlines())[1]
    return excerpt.strip()[:max_chars]


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



class UnicodeWriter(object):
    """A CSV writer that produces Excel-compatibly CSV files from unicode data.
    """
    def __init__(self, filename, **kwds):
        self.writer = csv.writer(open(filename, 'w'))

    def writerow(self, row):
        self.writer.writerow([unicode(col).encode('utf-8') for col in row])

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)

