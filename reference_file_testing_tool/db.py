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
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base
from .models import Files, COS, STIS, WFC3, ACS

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

def select_instrument_table(instrument):
    """Select and return SQL table object.

    Parameters
    ----------
    instrument: str
        instrument name 
    """
    instrument_list = {'COS':COS,
                       'STIS':STIS,
                       'WFC3':WFC3,
                       'ACS':ACS}

    return instrument_list[instrument]

def populate_instrument_table(instrument, db_path, num_cpu):
    """Populate the individual instrument tables for tool.

    Parameters
    ----------
    instrument: str
        instrument name
    db_path: str
        path to db
    num_cpu: int
        number of cpus to use in parallel
    """
    session = load_session(db_path)

    files = [os.path.join(result.path, result.filename)
                    for result in session.query(Files).\
                                    filter(Files.instrume == instrument)
            ]
    
    session.close()
    
    instrument_table = select_instrument_table(instrument)

    data_to_ingest = build_dask_delayed_list(instrument_table, files)

    print('EXTRACTING {} KEYWORDS'.format(instrument))
    with ProgressBar():
        data = compute(data_to_ingest, num_workers=num_cpu)[0]

    data_to_add = []
    for dataset in data:
        data_to_add.append(delayed(commit_session)(dataset, db_path))
    
    print('ADDING DATA TO {} DB'.format(instrument))
    with ProgressBar():
        compute(data_to_add, num_workers=num_cpu)

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
                 'rawtag.fits',
                 'rawtag_a.fits',
                 'rawtag_b.fits',
                 'raw.fits']

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
    
    expression = '[a-z]{2}\d{5}|cos|stis|adrizzle|acs'
    
    for item in os.listdir(top_dir):
        full_path = os.path.join(top_dir, item)
        pattern = re.compile((expression))
        if pattern.match(item) is not None:
            top_levels.append(full_path)
    
    results = build_dask_delayed_list(walk_filesystem, top_levels)

    with ProgressBar():
        final_paths = list(itertools.chain(*compute(results)[0]))

    return top_levels, final_paths

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
    for fname in glob.glob(file_path + '/*'):
        with fits.open(fname) as hdu:
            instrument = hdu[0].header['instrume']
        ins_table = select_instrument_table(instrument)
        new_test_data = ins_table(fname)
        session.add(new_test_data)
        session.commit()
        print("ADDED {} TO {} TABLE".format(file_path, instrument))
        
        # query_result = data_exists(fname, session)
        # if query_result.count() != 0 and not (force or replace):
        #     # If file exists and you don't want to force add or replace
        #     # let the user know this file is in the database and how
        #     # to add by force.
        #     print("There is already test data with the same parameters. \
        #         To add the data anyway use \
        #         db_utils force <db_path> <file_path>")
        # elif query_result and replace:
        #     session.delete(query_result.first())
        #     session.add(ins_table(fname))
        #     session.commit()
        #     print("REPLACED {} WITH {}".format(query_result.first().filename,
        #                                     file_path))
        # else:
        #     new_test_data = ins_table(fname)
        #     session.add(new_test_data)
        #     session.commit()
        #     print("ADDED {} TO {}".format(file_path, ins_table))


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
    
    data = build_dask_delayed_list(Files, full_file_paths)
    
    print("EXTRACTING KEYWORDS....")
    with ProgressBar():
        data_to_insert = compute(data, num_workers=num_cpu)[0]
    
    print("INSERTING INTO DB....")
    data_to_ingest = []
    for dataset in data_to_insert:
        data_to_ingest.append(delayed(commit_session)(dataset, db_path))

    with ProgressBar():
        compute(data_to_ingest, num_workers=num_cpu)
    
    hst_instruments = ['COS', 'STIS', 'WFC3', 'ACS']

    for instrume in hst_instruments:
        populate_instrument_table(instrume, db_path, num_cpu)

    # print("MAKING REGRESSION DB....")
    # reg_data = []
    # for directory in all_file_dirs:
    #     reg_data.append(delayed(add_test_data)(directory, db_path))
    
    # with ProgressBar():
    #     compute(reg_data, num_workers=num_cpu)

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