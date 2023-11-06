# MEDUSA

from setuptools import setup, find_packages
from codecs import open
from os import path

# Prevent publishing
import sys
argv = sys.argv
blacklist = [ 'register', 'upload' ]
for command in blacklist:
   if command in argv:
      values = {'command': command}
      print('Command "%(command)s" has been blacklisted, exiting...' % values)
      sys.exit(2)

here = path.abspath(path.dirname(__file__))

# Constants
NAME 			= 'medusa'
VERSION 		= '1.0.0'
HOMEPAGE 		= 'PRIVATE'
SHORT_DESCRIPTION 	= 'A module containing a superclass that abstracts multi process capabilities.'
AUTHOR 			= 'ed-roof-maker'
EMAIL 			= ''
LICENSE 		= 'PRIVATE'
MODULES 		= ['medusa']
MODULES_DIR		= { '' : 'lib'}
PKG_REQUIRES 		= ['rlab_common>=1.0.0']
DEV_STATUS		= 'Development Status :: 3 - Alpha'
AUDIENCE		= 'Intended Audience :: Developers'
TOPIC			= 'Topic :: Software Development :: Multi Process SuperClass'
KEYWORDS		= 'multiprocess worker-super-class'

# Get the long description from the relevant file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name=NAME,

    # Versions should comply with PEP440.  For a discussion on single-sourcing
    # the version across setup.py and the project code, see
    # https://packaging.python.org/en/latest/single_source_version.html
    version=VERSION,

    description=SHORT_DESCRIPTION,
    long_description=long_description,

    # The project's main homepage.
    url=HOMEPAGE,

    # Author details
    author=AUTHOR,
    author_email=EMAIL,

    # Choose your license
    license=LICENSE,

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        DEV_STATUS, 

        # Indicate who your project is intended for
        AUDIENCE,
        TOPIC,

        # Pick your license as you wish (should match "license" above)
        'License :: PRIVATE',

        # Specify the Python versions you support here. In particular, ensure
        # that you indicate whether you support Python 2, Python 3 or both.
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.6',
    ],

    # What does your project relate to?
    keywords=KEYWORDS,

    # You can just specify the packages manually here if your project is
    # simple. Or you can use find_packages().
    # packages=find_packages(exclude=['contrib', 'docs', 'tests*']),  
    # packages = ['minirepo'],
    py_modules=MODULES,

    package_dir=MODULES_DIR,

    # List run-time dependencies here.  These will be installed by pip when
    # your project is installed. For an analysis of "install_requires" vs pip's
    # requirements files see:
    # https://packaging.python.org/en/latest/requirements.html
    install_requires=PKG_REQUIRES,

    # To provide executable scripts, use entry points in preference to the
    # "scripts" keyword. Entry points provide cross-platform support and allow
    # pip to create the appropriate form of executable for the target platform.
    entry_points={
        'console_scripts': [
            NAME + '=' + NAME + ':main',
        ],
    },
)


