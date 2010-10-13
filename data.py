#
# Description: High level functions for extracting and storing data
# Author: Richard Penman (richard@sitescraper.net)
# License: LGPL
#

import csv
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
