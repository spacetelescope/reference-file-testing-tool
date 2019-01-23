"""Database utility scripts.

Usage:
  db_utils create <db_path>
  db_utils (add | replace | force | full_reg_set) <db_path> <file_path> [--num_cpu=<n>]

Arguments:
  <db_path>     Absolute path to database. 
  <file_path>   Absolute path to fits file to add. 

Options:
  -h --help         Show this screen.
  --version         Show version.
  --num_cpu=<n>     number of cpus to use [default: 2]
"""

import glob
import os

from astropy.io import fits
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from docopt import docopt
import itertools
import psutil
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

class RegressionData(Base):
    __tablename__ = 'regression_data'

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
    

    for root, dirs, files in os.walk(data_dir):
        full_paths = [os.path.join(root, filename) 
                      for filename in files 
                      if filename.endswith('uncal.fits')
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
    files = []

    for item in os.listdir(top_dir):
        full_path = os.path.join(top_dir, item)
        pattern = re.compile('[a-z]{2}\d{5}')
        if pattern.match(item) is not None and os.path.isdir(full_path):
            top_levels.append(full_path)
        elif pattern.match(item) and os.path.isfile(full_path):
            files.append(full_path)

    if top_levels:
        results = build_dask_delayed_list(walk_filesystem, top_levels)
        
        with ProgressBar():
            final_paths = list(itertools.chain(*compute(results)[0]))

        return top_levels, final_paths
    elif files:
        return top_levels, files


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
           
    query_result = session.query(RegressionData).filter_by(**args)
    return query_result


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

    # Handling depending on user input being path to direct file
    # or just file location.
    if os.path.isfile(file_path):
        files = [file_path]
    elif os.path.isdir(file_path):
        files = glob.glob(file_path + '/*')
    
    # For files in the path provided
    for fname in files:
        # Check if file exists in database
        if not fname.endswith('_uncal.fits'):
            continue
        else:
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
                session.add(RegressionData(fname))
                session.commit()
                print("REPLACED {} WITH {}".format(query_result.first().filename,
                                                file_path))
            else:
                new_test_data = RegressionData(fname)
                session.add(new_test_data)
                session.commit()
                print("ADDED {} TO DATABASE".format(file_path))


def bulk_populate(file_path, db_path, num_cpu):
    """Populate database in parallel.

    Parameters
    ----------
    file_path: str
        Data location
    db_path: str
        Absolute pat to database
    num_cpu: int
        Number of worker to pass dask.compute
        
    Returns
    -------
    None
    """
    
    print("GATHERING DATA, THIS CAN TAKE A FEW MINUTES....")
    all_file_dirs, full_file_paths = find_all_datasets(file_path)
    
    data = build_dask_delayed_list(TestData, full_file_paths)
    
    print("EXTRACTING KEYWORDS....")
    with ProgressBar():
        data_to_insert = compute(data, num_workers=num_cpu)[0]
        
    print("INSERTING INTO DB....")
    data_to_ingest = []
    for dataset in data_to_insert:
        data_to_ingest.append(delayed(commit_session)(dataset, db_path))
    
    with ProgressBar():
        compute(data_to_ingest, num_workers=num_cpu)
    
    for directory in all_file_dirs:
        add_test_data(directory, db_path)
    

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
        # Check to make sure user isn't exceeding number of CPUs.
        if int(args['--num_cpu']) > psutil.cpu_count():
                args = (psutil.cpu_count(), args['--num_cpu'])
                err_str = "YOUR MACHINE ONLY HAS {} CPUs! YOU ENTERED {}"       
                raise ValueError(err_str.format(*args))
        else:
            bulk_populate(args['<file_path>'],
                          args['<db_path>'],
                          int(args['--num_cpu']))