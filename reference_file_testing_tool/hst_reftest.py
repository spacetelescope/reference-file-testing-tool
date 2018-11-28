"""Script for testing hst reference files

Usage:
  test_hst_ref_file <ref_file> <db_path> [--data=<fname>] [--max_matches=<match>] [--num_cpu=<n>] [--email=<addr>]
  
Arguments:
  <db_path>     Absolute path to database. 
  <file_path>   Absolute path to fits file to add. 

Options:
  -h --help                  Show this screen.
  --version                  Show version.
  --data=<fname>             data to run pipeline with
  --max_matches=<match>      maximum number of data sets to test
  --num_cpu=<n>              number of cores to use [default: 2]
  --email=<addr>             email results from job with html table.
"""

from __future__ import print_function

# Make sure you can import calibration pipelines.
try:
    from acstools import calacs
    from calcos import calcos
    from stistools.calstis import calstis
    from wfc3tools import calwf3

except ImportError as e:
    print('MAKE SURE YOU ARE USING THE HST ASTROCONDA ENVIRONMENT!')
    print(e)

from astropy.io import fits
import crds
from crds.core import utils
from dask import compute, delayed
from dask.diagnostics import ProgressBar
from datetime import datetime
from docopt import docopt
from email.headerregistry import Address
from email.message import EmailMessage
from email.mime.text import MIMEText
import glob
import logging
import numpy as np
import os
import pandas as pd
import psutil
from shutil import copy
import smtplib
from sqlalchemy import or_

from .db import Files

def test_reference_file(ref_file, data_file):
    """Override CRDS reference file with the supplied reference file and run
    pipeline with supplied data file.
    
    Parameters
    ----------
    ref_file: str
        Path to reference file.
    data_file: str
        Path to data file.
    
    Returns
    -------
    result_meta: dict
        Dictionary with results from run.
    """

    # redirect pipeline log from sys.stderr to a string
    log_stream = StringIO()
    stpipe_log = logging.Logger.manager.loggerDict['stpipe']
    stpipe_log.handlers[0].stream = log_stream
    
    # allow invalid keyword values
    os.environ['PASS_INVALID_VALUES'] = '1'

    path, filename = os.path.split(data_file)
    result_meta = {'Path': path,
                   'Filename': filename}

    try:
        for pipeline in get_pipelines(fits.getheader(data_file)['EXP_TYPE']):
            pipeline = override_reference_file(ref_file, pipeline)
            pipeline.run(data_file)
        
        result_meta['Test_Status'] = 'PASSED'
        result_meta['Error_Msg'] = None
        
        return result_meta

    except Exception as err:
        result_meta['Test_Status'] = 'FAILED'
        result_meta['Error_Msg'] = err
        
        return result_meta


def send_email(data_for_email, addr):
    """Send nicely formatted pandas dataframe as html table via email when
    reference file test job is done.

    Parameters
    ----------
    data_for_email: list
        List of dictionaries to create dataframe out of
    addr: str
        Email address
    Returns
    -------
    None
    """
    
    # Make sure to strip the username from the domain if full email given.
    if '@' in addr:
        addr = addr.split("@")[0]
    
    # Make sure to print full error message...
    pd.set_option('display.max_colwidth', -1)
    
    # Make dataframe
    df = pd.DataFrame(data_for_email)
    
    # Make df into html table and then put into email.
    html_tb = df.to_html(justify='center',index=False)
    msg = EmailMessage()
    msg['Subject'] = 'Results From JWST Reference File Testing.'
    msg['From'] = Address('', addr, 'stsci.edu')
    msg['To'] = Address('', addr, 'stsci.edu')
    body_str = """
        <html>
            <head></head>
            <body>
                <p><b> Results from run </b></p>
                {}
            </body>
        </html>
        """.format(html_tb)
    msg.add_alternative(body_str, subtype='html')
     
    with smtplib.SMTP('smtp.stsci.edu') as s:
        s.send_message(msg)


def find_matches(ref_file, session):
    """Match and return HST files to calibrate.
    """

    header = fits.getheader(ref_file)

    instrume = header.get('INSTRUME')
    detector = header.get('DETECTOR')

    files_to_calibrate = [os.path.join(result.path, result.filename)
                            for result in session.query(Files).\
                                filter(Files.instrume == instrume).\
                                filter(Files.detector == detector)]
    
    return files_to_calibrate


def copy_files(files, ref_file, num_cpu):
    """copy files to calibrate to users local dir.
    """
    
    home = os.getenv('HOME')
    instrume, filetype = utils.get_file_properties('hst', ref_file)
    timestamp = datetime.now().strftime('%Y-%m-%dT%H-%M-%S')
    
    new_cal_dir = 'RFTT_{}_{}_{}'.format(instrume, filetype ,timestamp)
    new_cal_dir = os.path.join(home, new_cal_dir)
    os.mkdir(os.path.join(home, new_cal_dir))

    print("MOVING FILES TO CALIBRATE TO THIS LOCATION {}".format(new_cal_dir))

    files_to_copy = [delayed(copy)(f, new_cal_dir) for f in files[:10]]
    
    with ProgressBar():
        compute(files_to_copy, num_workers=num_cpu)
    
    files_to_update = [delayed(assign_ref_file)(f, ref_file, filetype) for f in glob.glob(new_cal_dir + '/*.fits')]

    print('ASSIGNING REFERENCE FILE TO HEADERS')
    with ProgressBar():
        compute(files_to_update, num_workers=num_cpu)


def assign_ref_file(filename, ref_file, filetype):
    """Assign reference file to headers for calibration
    """
    with fits.open(filename, mode='update') as hdu:
        if hdu[0].header[filetype]:
            hdu[0].header[filetype] = ref_file
        elif hdu[1].header[filetype]:
            hdu[1].header[filetype] = ref_file
        else:
            print("REFERENCE FILE ISNT IN EXT 0 OR 1")


def calibrate_files(pipeline, file_loc, outdir):
    """Calibrate files with appropriate pipeline
    """

    pipeline = get
    pipeline(file_loc)


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

    ref_file = args['<ref_file>']
    data_file = args['--data']
    num_cpu = int(args['--num_cpu'])
    
    session = db.load_session(db_path=args['<db_path>'])

    files = find_matches(ref_file, session)
    copy_files(files, ref_file, num_cpu)