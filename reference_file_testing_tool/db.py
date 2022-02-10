"""Database utility scripts.

Usage:
  db_utils create <db_path>
  db_utils (add | replace | force | full_reg_set | full_force) <db_path> <file_path> [--extension=<ext>] [--num_cpu=<n>] [--gen=<gn>]

Arguments:
  <db_path>     Absolute path to database.
  <file_path>   Absolute path to fits file to add.

Options:
  -h --help         Show this screen.
  --version         Show version.
  --num_cpu=<n>     number of cpus to use [default: 2]
  --extension=<ext>  extension [default: fits]
  --gen=<gn>       [default: 0]
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
    TEMPLATE = Column(String(50))
    DETECTOR = Column(String(20))
    BAND = Column(String(20))
    CHANNEL = Column(String(20))
    FILTER = Column(String(20))
    PUPIL = Column(String(20))
    GRATING = Column(String(20))
    NINTS = Column(String(20))
    NGROUPS = Column(String(20))
    SUBARRAY = Column(String(20))
    SUBSTRT1 = Column(String(20))
    SUBSTRT2 = Column(String(20))
    SUBSIZE1 = Column(String(20))
    SUBSIZE2 = Column(String(20))
    CORONMSK = Column(String(20))
    BKGDTARG = Column(String(3))
    TSOVISIT = Column(String(3))

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
        self.NINTS = header.get('NINTS')
        self.NGROUPS = header.get('NGROUPS')
        self.EXP_TYPE = header.get('EXP_TYPE')
        self.TEMPLATE = header.get('TEMPLATE')
        self.READPATT = header.get('READPATT')
        self.SUBARRAY = header.get('SUBARRAY')
        self.SUBSTRT1 = header.get('SUBSTRT1')
        self.SUBSTRT2 = header.get('SUBSTRT2')
        self.SUBSIZE1 = header.get('SUBSIZE1')
        self.SUBSIZE2 = header.get('SUBSIZE2')
        self.CORONMSK = header.get('CORONMSK',default='N/A')
        self.BKGDTARG = header.get('BKGDTARG')
        self.TSOVISIT = header.get('TSOVISIT')

class RegressionData(Base):
    __tablename__ = 'regression_data'

    filename = Column(String(100), primary_key=True)
    path = Column(String(200))
    DATE_OBS = Column(String(10))
    TIME_OBS = Column(String(12))
    INSTRUME = Column(String(20))
    READPATT = Column(String(20))
    EXP_TYPE = Column(String(20))
    TEMPLATE = Column(String(50))
    DETECTOR = Column(String(20))
    BAND = Column(String(20))
    CHANNEL = Column(String(20))
    FILTER = Column(String(20))
    PUPIL = Column(String(20))
    GRATING = Column(String(20))
    NINTS = Column(String(20))
    NGROUPS = Column(String(20))
    SUBARRAY = Column(String(20))
    SUBSTRT1 = Column(String(20))
    SUBSTRT2 = Column(String(20))
    SUBSIZE1 = Column(String(20))
    SUBSIZE2 = Column(String(20))
    CORONMSK = Column(String(20))
    BKGDTARG = Column(String(3))
    TSOVISIT = Column(String(3))


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
        self.NINTS = header.get('NINTS')
        self.NGROUPS = header.get('NGROUPS')
        self.EXP_TYPE = header.get('EXP_TYPE')
        self.TEMPLATE = header.get('TEMPLATE')
        self.READPATT = header.get('READPATT')
        self.SUBARRAY = header.get('SUBARRAY')
        self.SUBSTRT1 = header.get('SUBSTRT1')
        self.SUBSTRT2 = header.get('SUBSTRT2')
        self.SUBSIZE1 = header.get('SUBSIZE1')
        self.SUBSIZE2 = header.get('SUBSIZE2')
        self.CORONMSK= header.get('CORONMSK',default='N/A')
        self.BKGDTARG = header.get('BKGDTARG')
        self.TSOVISIT = header.get('TSOVISIT')

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
        engine = create_engine('sqlite:///{}'.format(db_path), echo=False, connect_args={'timeout': 15, 'check_same_thread':False})
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
        engine = create_engine('sqlite:///{}'.format(db_path), echo=False, connect_args={'timeout': 15, 'check_same_thread':False})
        Base.metadata.create_all(engine)


def build_dask_delayed_list(function, data, ver):
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
        if ver == 1:
           dask_delayed_list.append(delayed(function)(item))
        else:
           dask_delayed_list.append(delayed(function)(item,ver))


    return dask_delayed_list


def walk_filesystem(data_dir, extension):
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
                      if filename.endswith(extension)
                     ]
    return full_paths


def find_all_datasets(top_dir,extension,gen):
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

    print('the gen in here=',gen)
    for item in os.listdir(top_dir):
        full_path = os.path.join(top_dir, item)
        pattern = re.compile('[a-z]{2}\d{5}')

        if pattern.match(item) is not None and os.path.isdir(full_path):
            top_levels.append(full_path)
        elif pattern.match(item) and os.path.isfile(full_path):
            files.append(full_path)
        elif item.find(extension) > 0 and os.path.isfile(full_path):
            #top_levels.append(top_dir)
            files.append(full_path)
 


    if top_levels and not files:
        results = build_dask_delayed_list(walk_filesystem, top_levels, extension)
        with ProgressBar():
            final_paths = list(itertools.chain(*compute(results)[0]))
        return top_levels, final_paths
    elif files:
        files=glob.glob(top_dir+"/*"+extension)
        return top_levels, files
    elif gen == 1:
        w_card='*.'+extension
        files=[y for x in os.walk(top_dir) for y in glob.glob(os.path.join(x[0], w_card))]
        check_files(files)
        return top_levels, files

    print(top_levels, files)

def data_unique(fname, session):
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

    s1=fname.rfind('/')
    args1 = {}
    args1['filename'] = fname[s1+1:]

    query_result1 = session.query(RegressionData).filter_by(**args1)
    return query_result1

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
    args['NINTS'] = header.get('NINTS')
    args['NGROUPS'] = header.get('NGROUPS')
    args['EXP_TYPE'] = header.get('EXP_TYPE')
    args['TEMPLATE'] = header.get('TEMPLATE')
    args['READPATT'] = header.get('READPATT')
    args['SUBARRAY'] = header.get('SUBARRAY')
    args['SUBSTRT1'] = header.get('SUBSTRT1')
    args['SUBSTRT2'] = header.get('SUBSTRT2')
    args['SUBSIZE1'] = header.get('SUBSIZE1')
    args['SUBSIZE2'] = header.get('SUBSIZE2')
    args['CORONMSK'] = header.get('CORONMSK', default='N/A')
    args['BKGDTARG'] = header.get('BKGDTARG')
    args['TSOVISIT'] = header.get('TSOVISIT')

    query_result = session.query(RegressionData).filter_by(**args)
    return query_result


def add_test_data(file_path, db_path=None, force=False, replace=False, full_force=False, extension='*.fits'):
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
        files=walk_filesystem(file_path,extension)
        #files = glob.glob(file_path + '/*')

    all_file_paths=[]
    for fname in files:
        # Check if file exists in database
        query_result1 = data_unique(fname, session)

        if query_result1.count() != 0 :
            print('in add_test_data file already in DB: ',fname)
        else:
            #print('file NEW: ',fname)
            all_file_paths.append(fname)


    if len(all_file_paths) == 0:
        print('No data to add')
    else:
        print('')
        print(len(all_file_paths),' files to add')

    # For files in the path provided
    #for fname in all_file_paths:
    for fname in all_file_paths:
        # Check if file exists in database
        query_result1 = data_unique(fname, session)
        #if query_result1 ==8:
        if query_result1.count() != 0 :
           continue
        else:
           query_result = data_exists(fname, session)
           if query_result.count() != 0 and not (force or replace):
               # If file exists and you don't want to force add or replace
               # let the user know this file is in the database and how
               # to add by force.
               print("There is already test data with the same parameters. To force to add the data use db_utils force {} {} ".format(db_path,fname))
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
               print("ADDED {} TO DATABASE".format(fname))



def bulk_populate(force, file_path, db_path, num_cpu, extension, gen):
    """Populate database in parallel.

    Parameters
    ----------
    force: bolean
        True: Force to add all data in dir_path to DB
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

    # Looks for all the files with the provided extension in the find_all_datasets function
    all_file_dirs, full_file_paths = find_all_datasets(file_path,extension,gen)
    print('all files are =',all_file_dirs,full_file_paths)

    session = load_session(db_path)


    
    data = build_dask_delayed_list(RegressionData, full_file_paths,1)
    #all_file_paths)

    print("EXTRACTING KEYWORDS....")
    with ProgressBar():
        data_to_insert = compute(data, num_workers=num_cpu)[0]

    print("INSERTING INTO DB....")
    data_to_ingest = []
    for dataset in data_to_insert:
        data_to_ingest.append(delayed(commit_session)(dataset, db_path))

    #with ProgressBar():
    #    compute(data_to_ingest, num_workers=num_cpu)

    if not all_file_dirs:
        for file_path in full_file_paths:
            add_test_data(file_path, db_path, force=force, extension=extension)
    else:
        for directory in all_file_dirs:
            add_test_data(directory, db_path, force=force, extension=extension)

def check_files(filenames):

    for f in filenames:
       print(f)
       hdu=fits.open(f)
       header=hdu[0].header
       INSTRUME = header.get('INSTRUME')
       DETECTOR = header.get('DETECTOR')
       CHANNEL = header.get('CHANNEL')
       FILTER = header.get('FILTER')
       PUPIL = header.get('PUPIL')
       BAND = header.get('BAND')
       GRATING = header.get('GRATING')
       NINTS = header.get('NINTS')
       NGROUPS = header.get('NGROUPS')
       EXP_TYPE = header.get('EXP_TYPE')
       TEMPLATE = header.get('TEMPLATE')
       READPATT = header.get('READPATT')
       SUBARRAY = header.get('SUBARRAY')
       SUBSTRT1 = header.get('SUBSTRT1')
       SUBSTRT2 = header.get('SUBSTRT2')
       SUBSIZE1 = header.get('SUBSIZE1')
       SUBSIZE2 = header.get('SUBSIZE2')
       CORONMSK = header.get('CORONMSK', default='N/A')
       BKGDTARG = header.get('BKGDTARG')
       TSOVISIT = header.get('TSOVISIT')
       hdu.close()

                                  

def main():
    """Main to parse command line arguments.

    Parameters
    ----------
    None

    Returns
    -------
    None
    """

    # [--gen=<int>] --version
    # Get docopt arguments..
    args = docopt(__doc__, version='0.1')

    print(args['--gen'])
    # Parse command line arguments
    if args['create']:
        create_test_data_db(args['<db_path>'])
    elif args['add'] or args['force'] or args['replace']:
        add_test_data(args['<file_path>'],
                      db_path=args['<db_path>'],
                      force=args['force'],
                      replace=args['replace'])
    elif args['full_reg_set'] or args['full_force']:
        # Check to make sure user isn't exceeding number of CPUs.
        if int(args['--num_cpu']) > psutil.cpu_count():
                args = (psutil.cpu_count(), args['--num_cpu'])
                err_str = "YOUR MACHINE ONLY HAS {} CPUs! YOU ENTERED {}"
                raise ValueError(err_str.format(*args))
        else:
            if args['full_force']:
               bulk_populate(True,
                          args['<file_path>'],
                          args['<db_path>'],
                          int(args['--num_cpu']),
                          args['--extension'],
                          int(args['--gen']))
            else:
               bulk_populate(False,args['<file_path>'],
                          args['<db_path>'],
                          int(args['--num_cpu']),
                          args['--extension'],
                          int(args['--gen']))
