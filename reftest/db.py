# from . import log

from astropy.io import fits
import glob
import os
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
# import logging

__all__ = ['TestData', 'data_exists', 'load_session', 'create_test_data_db',
           'add_test_data']

# log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)

Base = declarative_base()

REFTEST_DATA_DB = os.environ.get('REFTEST_DB')

class TestData(Base):
    __tablename__ = 'test_data'
    # Here we define columns for the exposures table which will just contain
    # some basic fits header information.
    id = Column(Integer, primary_key=True)
    filename = Column(String(250))
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

    def __init__(self, filename):
        # you don't have to create an __init__()
        # but it makes it easier to create a new row
        # from a FITS file
        self.filename = filename
        header = fits.getheader(filename)
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
        True if there is no 
        
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
    query_result = session.query(TestData).filter_by(**args)
    return bool(query_result.count())


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
        if REFTEST_DATA_DB is None:
            print('REFTEST_DATA_DB is None, please specify a database')
            return None
        else:
            db_path = REFTEST_DATA_DB

    engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
    print('Connected to DB at {}'.format(db_path))
    return session


def create_test_data_db(db_path):
    """
    Create the SQLite DB for test data.
    
    Parameters
    ----------
    db_path: str
        Where to save the DB
    """
    engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
    Base.metadata.create_all(engine)


def add_test_data(file_path, db_path=None, force=False):
    """
    Add files to the test data DB.
    Parameters
    ----------
    file_path: str
        Globable file string for test data
    """

    session = load_session(db_path)
    for fname in glob.glob(file_path):
        if data_exists(fname, session) and not force:
            print('There is already test data with the same parameters. To add the data anyway set force=True')
        else:
            new_test_data = TestData(fname)
            session.add(new_test_data)
            session.commit()

