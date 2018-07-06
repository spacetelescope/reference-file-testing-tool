"""Database utility scripts.

Usage:
  db_utils (create | remove) <db_path>
  db_utils (add | replace | force) <db_path> <file_path>

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
from docopt import docopt
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
        # Populates row when instance of class is envoked.
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

    # set up a session with the database
    if db_path is None:
        print("db_path = None, SUPPLY ABSOLUTE PATH TO DB!")
    else:
        engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
        Session = sessionmaker(bind=engine)
        session = Session()
        print("Connected to DB at {}".format(db_path))
        return session


def create_test_data_db(db_path):
    """
    Create the SQLite DB for test data.
    
    Parameters
    ----------
    db_path: str
        Absolute path to save the DB
    """

    engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
    Base.metadata.create_all(engine)
    print("CREATED DB: {}".format(db_path))


def add_test_data(file_path, db_path=None, force=False, replace=False):
    """
    Add files to the test data DB.
    
    Parameters
    ----------
    file_path: str
        Absolute path for data file to add
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

    session = load_session(db_path)
    for fname in glob.glob(file_path):
        query_result = data_exists(fname, session)
        if query_result.count() != 0 and not (force or replace):
            print("There is already test data with the same parameters. To add the data anyway use 'db_utils force <db_path> <file_path>'")
        elif query_result and replace:
            session.delete(query_result.first())
            session.add(TestData(fname))
            session.commit()
            print("Replaced {} with {}".format(query_result.first().filename,
                                               file_path))
        else:
            new_test_data = TestData(fname)
            session.add(new_test_data)
            session.commit()
            print("Added {} to database".format(file_path))


def main():
    """Main to parse command line entries.

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
    else:
        # Maybe try to open a session with file and if it fails, don't delete
        # Else, delete.
        if args['<db_path>'].endswith('.db'):
            os.remove(args['<db_path>'])
            print("DELETED DB {}".format(args['<db_path>']))
        else:
            print('TO DELETE DB, FILE NEEDS TO END WITH ".db"')