__doc__ = """
Description: webscraping library
Website: http://code.google.com/p/webscraping/
License: LGPL
"""

if __name__ == '__main__':
    import doctest
    for name in ['adt', 'alg', 'common', 'download', 'pdict', 'settings', 'webkit', 'xpath']:
        module = __import__(name)
        print name
        print doctest.testmod(module)
