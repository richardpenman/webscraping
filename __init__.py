import sys
import os

if __name__ == '__main__':
    import doctest
    import common, data, download, xpath
    for module in (common, data, xpath):
        print doctest.testmod(module)
