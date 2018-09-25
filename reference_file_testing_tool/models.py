"""SQLALCHEMY DB models for RFTT.
"""

from astropy.io import fits
import os
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Files(Base):
    __tablename__ = 'files'

    filename = Column(String(100), primary_key=True)
    path = Column(String(200))
    date_obs = Column(String(30))
    time_obs = Column(String(30))
    telescop = Column(String(10))
    instrume = Column(String(10))
    detector = Column(String(10))
    
    def __init__(self, filename):
        header = fits.getheader(filename)
        path, name = os.path.split(filename)

        self.filename = name
        self.path = path
        self.date_obs = header.get('DATE-OBS')
        self.time_obs = header.get('TIME-OBS')
        self.telescop = header.get('TELESCOP')
        self.instrume = header.get('INSTRUME')
        self.detector = header.get('DETECTOR')

class COS(Base):
    __tablename__ = 'hst_cos'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    cenwave = Column(Integer)
    exptype = Column(String(20))
    life_adj = Column(Integer)
    obsmode = Column(String(10))
    opt_elem = Column(String(10))
    
    def __init__(self, filename):
        header = fits.getheader(filename)
        name = os.path.split(filename)[1]

        self.filename = name
        self.cenwave = header.get('CENWAVE')
        self.exptype = header.get('EXPTYPE')
        self.life_adj = header.get('LIFE_ADJ')
        self.obsmode = header.get('OBSMODE')
        self.opt_elem = header.get('OPT_ELEM')

class STIS(Base):
    __tablename__ = 'hst_stis'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    aperture = Column(String(10))
    binaxis1 = Column(Integer)
    binaxis2 = Column(Integer)
    ccdamp = Column(String(10))
    ccdgain = Column(Integer)
    ccdoffst = Column(Integer)
    cenwave = Column(Integer)
    obstype = Column(String(10))
    opt_elem = Column(String(10))
    
    def __init__(self, filename):
        header = fits.getheader(filename)
        name = os.path.split(filename)[1]

        self.filename = name
        self.aperture = header.get('APERTURE')
        self.binaxis1 = header.get('BINAXIS1')
        self.binaxis2 = header.get('BINAXIS2')
        self.ccdamp = header.get('CCDAMP')
        self.ccdgain = header.get('CCDGAIN')
        self.ccdoffst = header.get('CCDOFFST')
        self.cenwave = header.get('CENWAVE')
        self.obstype = header.get('OBSTYPE')
        self.opt_elem = header.get('OPT_ELEM')

class WFC3(Base):
    __tablename__ = 'hst_wfc3'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    aperture = Column(String(20))
    binaxis1 = Column(Integer)
    binaxis2 = Column(Integer)
    ccdamp = Column(Integer)
    ccdgain = Column(Integer)
    chinject = Column(Integer)
    filter = Column(String(20))
    flashcur = Column(String(20))
    sample_seq = Column(String(20))
    shutrpos = Column(String(20))
    subarray = Column(String(10))
    subtype = Column(String(20)) 
    
    def __init__(self, filename):
        header = fits.getheader(filename)
        name = os.path.split(filename)[1]

        self.filename = name
        self.aperture =  header.get('APERTURE')   
        self.binaxis1 = header.get('BINAXIS1')
        self.binaxis2 = header.get('BINAXIS2')
        self.ccdamp = header.get('CCDAMP')
        self.ccdgain = header.get('CCDGAIN')
        self.chinject = header.get('CHINJECT')
        self.filter = header.get('FILTER')
        self.flashcur = header.get('FLASHCUR')
        self.sample_seq = header.get('SAMPLE_SEQ') 
        self.shutrpos = header.get('SHUTRPOS')
        self.subarray = header.get('SUBARRAY')
        self.subtype =  header.get('SUBTYPE')

class ACS(Base):
    __tablename__ = 'hst_acs'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    aperture = Column(String(20))
    ccdamp = Column(Integer)
    ccdgain = Column(Integer)
    filter1 = Column(String(20))
    filter2 = Column(String(20))
    obstype = Column(String(20))
    flashcurr = Column(String(20))
    shutrpos = Column(String(20))
    fw1offst = Column(String(20))
    fw2offst = Column(String(20))
    fwsoffst = Column(String(20))
    ltv1 = Column(String(20))
    ltv2 = Column(String(20))
    naxis1 = Column(Integer)
    naxis2 = Column(Integer)
    xcorner = Column(String(20))
    ycorner = Column(String(20))
    
    def __init__(self, filename):
        header = fits.getheader(filename)
        name = os.path.split(filename)[1]

        self.filename = name
        self.aperture = header.get('APERTURE')
        self.ccdamp = header.get('CCDAMP')
        self.ccdgain = header.get('CCDGAIN')
        self.filter1 = header.get('FILTER1')
        self.filter2 = header.get('FILTER2')
        self.obstype = header.get('OBSTYPE')
        self.flashcurr =header.get('FLASHCURR')
        self.shutrpos = header.get('SHUTRPOS')
        self.fw1offst = header.get('FW1OFFST')
        self.fw2offst = header.get('FW2OFFST')
        self.fwsoffst = header.get('FWSOFFST')
        self.ltv1 = header.get('LTV1')
        self.ltv2 = header.get('LTV2')
        self.naxis1 = header.get('NAXIS1')
        self.naxis2 = header.get('NAXIS2')
        self.xcorner = header.get('XCORNER')
        self.ycorner = header.get('YCORNER')
        

# class TestData(Base):
#     __tablename__ = 'test_data'

#     filename = Column(String(100), primary_key=True)
#     path = Column(String(200))
#     DATE_OBS = Column(String(10))
#     TIME_OBS = Column(String(12))
#     INSTRUME = Column(String(20))
#     READPATT = Column(String(20))
#     EXP_TYPE = Column(String(20))
#     DETECTOR = Column(String(20))
#     BAND = Column(String(20))
#     CHANNEL = Column(String(20))
#     FILTER = Column(String(20))
#     PUPIL = Column(String(20))
#     GRATING = Column(String(20))
#     SUBARRAY = Column(String(20))
#     SUBSTRT1 = Column(String(20))
#     SUBSTRT2 = Column(String(20))
#     SUBSIZE1 = Column(String(20))
#     SUBSIZE2 = Column(String(20))


#     def __init__(self, filename):
#         header = fits.getheader(filename)
#         path, name = os.path.split(filename)
        
#         self.filename = name
#         self.path = path
#         self.DATE_OBS = header.get('DATE-OBS')
#         self.TIME_OBS = header.get('TIME-OBS')
#         self.INSTRUME = header.get('INSTRUME')
#         self.DETECTOR = header.get('DETECTOR')
#         self.CHANNEL = header.get('CHANNEL')
#         self.FILTER = header.get('FILTER')
#         self.PUPIL = header.get('PUPIL')
#         self.BAND = header.get('BAND')
#         self.GRATING = header.get('GRATING')
#         self.EXP_TYPE = header.get('EXP_TYPE')
#         self.READPATT = header.get('READPATT')
#         self.SUBARRAY = header.get('SUBARRAY')
#         self.SUBSTRT1 = header.get('SUBSTRT1')
#         self.SUBSTRT2 = header.get('SUBSTRT2')
#         self.SUBSIZE1 = header.get('SUBSIZE1')
#         self.SUBSIZE2 = header.get('SUBSIZE2')

# class RegressionData(Base):
#     __tablename__ = 'regression_data'

#     filename = Column(String(100), primary_key=True)
#     path = Column(String(200))
#     DATE_OBS = Column(String(10))
#     TIME_OBS = Column(String(12))
#     INSTRUME = Column(String(20))
#     READPATT = Column(String(20))
#     EXP_TYPE = Column(String(20))
#     DETECTOR = Column(String(20))
#     BAND = Column(String(20))
#     CHANNEL = Column(String(20))
#     FILTER = Column(String(20))
#     PUPIL = Column(String(20))
#     GRATING = Column(String(20))
#     SUBARRAY = Column(String(20))
#     SUBSTRT1 = Column(String(20))
#     SUBSTRT2 = Column(String(20))
#     SUBSIZE1 = Column(String(20))
#     SUBSIZE2 = Column(String(20))


#     def __init__(self, filename):
#         header = fits.getheader(filename)
#         path, name = os.path.split(filename)
        
#         self.filename = name
#         self.path = path
#         self.DATE_OBS = header.get('DATE-OBS')
#         self.TIME_OBS = header.get('TIME-OBS')
#         self.INSTRUME = header.get('INSTRUME')
#         self.DETECTOR = header.get('DETECTOR')
#         self.CHANNEL = header.get('CHANNEL')
#         self.FILTER = header.get('FILTER')
#         self.PUPIL = header.get('PUPIL')
#         self.BAND = header.get('BAND')
#         self.GRATING = header.get('GRATING')
#         self.EXP_TYPE = header.get('EXP_TYPE')
#         self.READPATT = header.get('READPATT')
#         self.SUBARRAY = header.get('SUBARRAY')
#         self.SUBSTRT1 = header.get('SUBSTRT1')
#         self.SUBSTRT2 = header.get('SUBSTRT2')
#         self.SUBSIZE1 = header.get('SUBSIZE1')
#         self.SUBSIZE2 = header.get('SUBSIZE2')