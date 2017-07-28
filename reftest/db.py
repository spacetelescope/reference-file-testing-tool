from astropy.io import fits
import glob
import os
from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

REFTEST_DATA_DB = os.environ.get('REFTEST_DB')

class TestData(Base):
    __tablename__ = 'test_data'
    # Here we define columns for the exposures table which will just contain
    # some basic fits header information.
    id = Column(Integer, primary_key=True)
    filename = Column(String(250))

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


def load_session(db_path):
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
    engine = create_engine('sqlite:///{}'.format(db_path), echo=False)
    metadata = Base.metadata
    Session = sessionmaker(bind=engine)
    session = Session()
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


def add_test_data(db_path, file_path):
    """
    Add files to the test data DB.
    Parameters
    ----------
    file_path: str
        Globable file string for test data
    """

    session = load_session(db_path)
    for fname in glob.glob(file_path):
        new_test_data = TestData(fname)
        session.add(new_test_data)
        session.commit()


def main(args=None):
    from astropy.utils.compat import argparse

    parser = argparse.ArgumentParser(
        description="Check that a reference file runs in the calibration pipeline",
        )
    parser.add_argument(
        'test_data', help='Globable file string for test data')
    parser.add_argument(
        '--db_path', help='Path to save the SQLite DB',
                        default=REFTEST_DATA_DB)

    res = parser.parse_args(args)
    create_test_data_db(res.db_path)
