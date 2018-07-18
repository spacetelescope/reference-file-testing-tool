"""Database utility scripts.

Usage:
  db_utils (create | remove) <db_path>
  db_utils (add | replace | force | full_reg_set) <db_path> <file_path>

Arguments:
  <db_path>     Absolute path to database. 
  <file_path>   Absolute path to fits file to add. 

Options:
  -h --help     Show this screen.
  --version     Show version.
"""

import glob
import os

from astropy.io import fits
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from docopt import docopt
import itertools
import re
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class TestData(Base):
    __tablename__ = 'test_data'

    filename = Column(String(100), primary_key=True)
    path = Column(String(200))
    DATE_OBS = Column(String(10))
    TIME_OBS = Column(String(12))
    INSTRUME = Column(String(20))
    READPATT = Column(String(20))
    EXP_TYPE = Column(String(20))
    DETECTOR = Column(String(20))
    BAND = Column(String(20))
    CHANNEL = Column(String(20))
    FILTER = Column(String(20))
    PUPIL = Column(String(20))
    GRATING = Column(String(20))
    SUBARRAY = Column(String(20))
    SUBSTRT1 = Column(String(20))
    SUBSTRT2 = Column(String(20))
    SUBSIZE1 = Column(String(20))
    SUBSIZE2 = Column(String(20))


    def __init__(self, filename):
        header = fits.getheader(filename)
        path, name = os.path.split(filename)
        
        self.filename = name
        self.path = path
        self.DATE_OBS = header.get('DATE-OBS')
        self.TIME_OBS = header.get('TIME-OBS')
        self.INSTRUME = header.get('INSTRUME')
        self.DETECTOR = header.get('DETECTOR')
        self.CHANNEL = header.get('CHANNEL')
        self.FILTER = header.get('FILTER')
        self.PUPIL = header.get('PUPIL')
        self.BAND = header.get('BAND')
        self.GRATING = header.get('GRATING')
        self.EXP_TYPE = header.get('EXP_TYPE')
        self.READPATT = header.get('READPATT')
        self.SUBARRAY = header.get('SUBARRAY')
        self.SUBSTRT1 = header.get('SUBSTRT1')
        self.SUBSTRT2 = header.get('SUBSTRT2')
        self.SUBSIZE1 = header.get('SUBSIZE1')
        self.SUBSIZE2 = header.get('SUBSIZE2')
    
def build_dask_delayed_list(function, data):
    """Build list of dask delayed objects for functions with single arguments.
    May want to expand for more arguments in the future.

    Parameters
    ----------
    function: func
        Function to multiprocess
    data: list-like
        List of data passed to function.
    
    Returns
    -------
    dask_delayed_list: list
        List of dask delayed objects to run in parallel.
    """
    
    dask_delayed_list = []

    for item in data:
        dask_delayed_list.append(delayed(function)(item))

    return dask_delayed_list


def walk_filesystem(data_dir):
    """Wrapper for os.walk to return files in directorys
    below root.
    
    Parameters
    ----------
    data_dir: str
        Directory to look for files in.
    
    Returns
    -------
    full_path: list
        List absolute paths to files in side of data_dir
    """
    # We only want uncalibrabrated products.
    filetypes = ['uncal.fits',
                 'rate.fits',
                 'rateints.fits',
                 'trapsfilled.fits',
                 'dark.fits']

    for root, dirs, files in os.walk(data_dir):
        # Join path + filename for files if extension is in filetypes.
        full_paths = [os.path.join(root, filename) 
                      for filename in files 
                      if any(
                             filetype in filename 
                             for filetype in filetypes
                             )
                     ]
    
    return full_paths

def find_all_datasets(top_dir):
    """Crawl through the JWST test regression datasystem
    to locate files.

    Parameters
    ----------
    top_dir: str
        top level dir to crawl down from.
    
    Returns
    -------
    final_paths: list
        A list of all the files that were located.
    """
    
    top_levels = []
    
    for item in os.listdir(top_dir):
        full_path = os.path.join(top_dir, item)
        pattern = re.compile('[a-z]{2}\d{5}')
        if pattern.match(item) is not None:
            top_levels.append(full_path)
    
    results = build_dask_delayed_list(walk_filesystem, top_levels)
    
    with ProgressBar():
        final_paths = list(itertools.chain(*compute(results)[0]))

    return final_paths

def bulk_populate(file_path, db_path):
    """Populate database with in parallel.

    Parameters
    ----------
    file_path: str
        Data location
    db_path: str
        Absolute pat to database
    
    Returns
    -------
    None
    """
    
    print("GATHERING DATA, THIS CAN TAKE A FEW MINUTES....")
    final_paths = find_all_datasets(file_path)
    
    data = build_dask_delayed_list(TestData, final_paths)
    
    print("EXTRACTING KEYWORDS....")s
    with ProgressBar():
        data_to_insert = compute(data)[0]
    
    data_to_ingest = []
    for dataset in data_to_insert:
        data_to_ingest.append(delayed(commit_session)(dataset, db_path))
        
    print("INSERTING INTO DB....")
    with ProgressBar():
        compute(data_to_ingest)

def data_exists(fname, session):
    """
    Check if there is already a dataset with the proposed dataset's parameters
    
    Parameters
    ----------
    fname: str
        proposed new file
    session: sqlalchemy.Session
        DB Session

    Returns
    -------
        True if there are no matches
    """

    header = fits.getheader(fname)
    args = {}
    args['INSTRUME'] = header.get('INSTRUME')
    args['DETECTOR'] = header.get('DETECTOR')
    args['CHANNEL'] = header.get('CHANNEL')
    args['FILTER'] = header.get('FILTER')
    args['PUPIL'] = header.get('PUPIL')
    args['BAND'] = header.get('BAND')
    args['GRATING'] = header.get('GRATING')
    args['EXP_TYPE'] = header.get('EXP_TYPE')
    args['READPATT'] = header.get('READPATT')
    args['SUBARRAY'] = header.get('SUBARRAY')
    args['SUBSTRT1'] = header.get('SUBSTRT1')
    args['SUBSTRT2'] = header.get('SUBSTRT2')
    args['SUBSIZE1'] = header.get('SUBSIZE1')
    args['SUBSIZE2'] = header.get('SUBSIZE2')
           
    query_result = session.query(TestData).filter_by(**args)
    return query_result


def load_session(db_path=None):
    """
    Create a new session with the test data DB.
    
    Parameters
    ----------
    db_path: str
        Path to test data DB.

    Returns
    -------
    session: sqlalchemy.orm.Session
    """

    if db_path is None:
        print("db_path = None, SUPPLY ABSOLUTE PATH TO DB!")
    else:
        engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        return session

def commit_session(data, db_path):
    """Load and commit additions to DB

    Parameters
    ----------
    data: object
        Instance of TestData
    db_path: str
        Location of database
    """
    
    session = load_session(db_path)
    session.add(data)
    session.commit()


def create_test_data_db(db_path):
    """
    Create the SQLite DB for test data.
    
    Parameters
    ----------
    db_path: str
        Absolute path to save the DB
    """

    if os.path.exists(db_path):
        print("{} EXISTS ALREADY!".format(db_path))
    else:
        engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
        Base.metadata.create_all(engine)


def add_test_data(file_path, db_path=None, force=False, replace=False):
    """
    Add files to the test data DB.
    
    Parameters
    ----------
    file_path: str
        Absolute path for data file to add or data location
    db_path: str
        Absolute path to database on local machince
    force: bool
        Force add to db even if file shares same field entries
    replace: bool
        Replace file in database with file_path

    Returns
    -------
    None
    """

    # Create DB session
    session = load_session(db_path)

    # For files in the path provided
    for fname in glob.glob(file_path):
        # Check if file exists in database
        query_result = data_exists(fname, session)
        if query_result.count() != 0 and not (force or replace):
            # If file exists and you don't want to force add or replace
            # let the user know this file is in the database and how
            # to add by force.
            print("There is already test data with the same parameters. \
                   To add the data anyway use \
                   db_utils force <db_path> <file_path>")
        elif query_result and replace:
            session.delete(query_result.first())
            session.add(TestData(fname))
            session.commit()
            print("REPLACED {} WITH {}".format(query_result.first().filename,
                                               file_path))
        else:
            new_test_data = TestData(fname)
            session.add(new_test_data)
            session.commit()
            print("ADDED {} TO DATABASE".format(file_path))


def main():
    """Main to parse command line arguments.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # Get docopt arguments..
    args = docopt(__doc__, version='0.1')

    # Parse command line arguments
    if args['create']:
        create_test_data_db(args['<db_path>'])
    elif args['add'] or args['force'] or args['replace']:
        add_test_data(args['<file_path>'], 
                      db_path=args['<db_path>'], 
                      force=args['force'], 
                      replace=args['replace'])
    elif args['full_reg_set']:
        bulk_populate(args['<file_path>'],
                      args['<db_path>'])
    else:
        print("Needs some work....")