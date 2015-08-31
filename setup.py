import os
from distutils.core import setup

def read(filename):
    return open(os.path.join(os.path.dirname(__file__), filename)).read()

setup(
    name='webscraping', 
    version='1.5',
    packages=['webscraping'],
    package_dir={'webscraping':'.'}, # look for package contents in current directory
    author='Richard Penman',
    author_email='richard@webscraping.com',
    description='Pure python library aimed to make web scraping easier',
    long_description=read('README.rst'),
    url='http://bitbucket.org/richardpenman/webscraping',
    classifiers = [
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Internet :: WWW/HTTP'
    ],
    license='lgpl'
)
