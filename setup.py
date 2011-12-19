from distutils.core import setup
setup(
    name='webscraping', 
    version='1',
    packages=['webscraping'],
    package_dir={'webscraping':'.'}, # look for package contents in current directory
    author='Richard Penman',
    author_email='richard@sitescraper.net',
    description='Pure python library aimed to make web scraping easier',
    url='http://code.google.com/p/webscraping',
    license='lgpl',
)
