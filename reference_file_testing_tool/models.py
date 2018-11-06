"""SQLALCHEMY DB models for RFTT.
"""

from astropy.io import fits
from collections import ChainMap
import numpy as np
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
        path, name = os.path.split(filename)
        with fits.open(filename) as hdu:
            self.filename = name
            self.path = path
            self.instrume = hdu[0].header.get('INSTRUME')
            
            # Time and date keywords have different names or can appear
            # in different extensions.
            if self.instrume == 'STIS':
                date_key = 'TDATEOBS'
                time_key = 'TTIMEOBS'
                ext=0
            elif self.instrume == 'COS':
                date_key = 'DATE-OBS'
                time_key = 'TIME-OBS'
                ext=1
            else:
                date_key = 'DATE-OBS'
                time_key = 'TIME-OBS'
                ext=0
            
            self.date_obs = hdu[ext].header.get(date_key)
            self.time_obs = hdu[ext].header.get(time_key)
            self.telescop = hdu[0].header.get('TELESCOP')
            self.detector = hdu[0].header.get('DETECTOR')

class COS(Base):
    __tablename__ = 'hst_cos'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    cenwave = Column(Integer)
    exptype = Column(String(20))
    life_adj = Column(Integer)
    obsmode = Column(String(10))
    opt_elem = Column(String(10))
    
    def __init__(self, filename):
        name = os.path.split(filename)[1]
        self.filename = name
        with fits.open(filename) as hdu:
            self.cenwave = hdu[0].header.get('CENWAVE')
            self.exptype = hdu[0].header.get('EXPTYPE')
            self.life_adj = hdu[0].header.get('LIFE_ADJ')
            self.obsmode = hdu[0].header.get('OBSMODE')
            self.opt_elem = hdu[0].header.get('OPT_ELEM')

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
        name = os.path.split(filename)[1]
        self.filename = name
        with fits.open(filename) as hdu:
            self.aperture = hdu[0].header.get('APERTURE')
            self.binaxis1 = hdu[0].header.get('BINAXIS1')
            self.binaxis2 = hdu[0].header.get('BINAXIS2')
            self.ccdamp = hdu[0].header.get('CCDAMP')
            self.ccdgain = hdu[0].header.get('CCDGAIN')
            self.ccdoffst = hdu[0].header.get('CCDOFFST')
            self.cenwave = hdu[0].header.get('CENWAVE')
            self.obstype = hdu[0].header.get('OBSTYPE')
            self.opt_elem = hdu[0].header.get('OPT_ELEM')

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
        name = os.path.split(filename)[1]
        self.filename = name
        with fits.open(filename) as hdu:
            self.aperture =  hdu[0].header.get('APERTURE')   
            self.binaxis1 = hdu[1].header.get('BINAXIS1')
            self.binaxis2 = hdu[1].header.get('BINAXIS2')
            self.ccdamp = hdu[0].header.get('CCDAMP')
            self.ccdgain = hdu[0].header.get('CCDGAIN')
            self.chinject = hdu[0].header.get('CHINJECT') # NOT CONSITENT ACROSS MODES
            self.filter = hdu[0].header.get('FILTER')
            self.flashcur = hdu[0].header.get('FLASHCUR') # NOT CONSITENT ACROSS MODES
            self.sample_seq = hdu[0].header.get('SAMPLE_SEQ') # NOT CONSITENT ACROSS MODES
            self.shutrpos = hdu[0].header.get('SHUTRPOS') # NOT CONSITENT ACROSS MODES
            self.subarray = hdu[0].header.get('SUBARRAY')
            self.subtype =  hdu[0].header.get('SUBTYPE')

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
        
        name = os.path.split(filename)[1]
        self.filename = name
        with fits.open(filename) as hdu:
            self.aperture = hdu[0].header.get('APERTURE')
            self.ccdamp = hdu[0].header.get('CCDAMP')
            self.ccdgain = hdu[0].header.get('CCDGAIN')
            self.filter1 = hdu[0].header.get('FILTER1')
            self.filter2 = hdu[0].header.get('FILTER2')
            self.obstype = hdu[0].header.get('OBSTYPE')
            self.flashcurr = hdu[0].header.get('FLASHCURR') # NOT CONSITENT ACROSS MODES
            self.shutrpos = hdu[0].header.get('SHUTRPOS')
            self.fw1offst = hdu[0].header.get('FW1OFFST')
            self.fw2offst = hdu[0].header.get('FW2OFFST')
            self.fwsoffst = hdu[0].header.get('FWSOFFST')
            self.ltv1 = hdu[1].header.get('LTV1')
            self.ltv2 = hdu[1].header.get('LTV2')
            self.naxis1 = hdu[1].header.get('NAXIS1')
            self.naxis2 = hdu[1].header.get('NAXIS2')
            self.xcorner = hdu[0].header.get('XCORNER') # NOT CONSITENT ACROSS MODES
            self.ycorner = hdu[0].header.get('YCORNER') # NOT CONSITENT ACROSS MODES
        

class JWST(Base):
    __tablename__ = 'jwst'

    filename = Column(String(100), ForeignKey(Files.filename), primary_key=True)
    READPATT = Column(String(20))
    EXP_TYPE = Column(String(20))
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
        
        name = os.path.split(filename)[1]
        self.filename = name
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