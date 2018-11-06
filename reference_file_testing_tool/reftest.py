"""Script for testing reference files

Usage:
  test_ref_file <ref_file> <db_path> [--data=<fname>] [--max_matches=<match>] [--num_cpu=<n>] [--email=<addr>]
  
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
from jwst import datamodels
from jwst.pipeline import calwebb_dark, calwebb_image2, calwebb_spec2

try:
    from jwst.pipeline import SloperPipeline as Detector1Pipeline
except ImportError:
    from jwst.pipeline import Detector1Pipeline

import logging
import numpy as np
import os
import pandas as pd
import psutil
from shutil import copy
import smtplib
from sqlalchemy import or_

# Remove python 2 dependencies in the future..
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from . import db
from .models import Files, COS, STIS, WFC3, ACS

p_mapping = {
    "META.EXPOSURE.TYPE": "META.EXPOSURE.P_EXPTYPE",
    "META.INSTRUMENT.BAND": "META.INSTRUMENT.P_BAND",
    "META.INSTRUMENT.DETECTOR": "META.INSTRUMENT.P_DETECTOR",
    "META.INSTRUMENT.CHANNEL": "META.INSTRUMENT.P_CHANNEL",
    "META.INSTRUMENT.FILTER": "META.INSTRUMENT.P_FILTER",
    "META.INSTRUMENT.PUPIL": "META.INSTRUMENT.P_PUPIL",
    "META.INSTRUMENT.MODULE": "META.INSTRUMENT.P_MODULE",
    "META.SUBARRAY.NAME": "META.SUBARRAY.P_SUBARRAY",
    "META.INSTRUMENT.GRATING": "META.INSTRUMENT.P_GRATING",
    "META.EXPOSURE.READPATT": "META.EXPOSURE.P_READPATT"
}


meta_to_fits = {
    'META.INSTRUMENT.NAME': 'INSTRUME',
    'META.EXPOSURE.READPATT': 'READPATT',
    'META.EXPOSURE.TYPE': 'EXP_TYPE',
    'META.INSTRUMENT.BAND': 'BAND',
    'META.INSTRUMENT.CHANNEL': 'CHANNEL',
    'META.INSTRUMENT.DETECTOR': 'DETECTOR',
    'META.INSTRUMENT.FILTER': 'FILTER',
    'META.INSTRUMENT.GRATING': 'GRATING',
    'META.SUBARRAY.NAME': 'SUBARRAY'
}


IMAGING = ['fgs_image', 'fgs_focus', 'fgs_skyflat', 'fgs_intflat', 'mir_image',
           'mir_tacq', 'mir_lyot', 'mir_4qpm', 'mir_coroncal', 'nrc_image',
           'nrc_tacq', 'nrc_coron', 'nrc_taconfirm', 'nrc_focus', 'nrc_tsimage',
           'nis_image', 'nis_dark', 'nis_ami', 'nis_tacq', 'nis_taconfirm', 'nis_focus',
           'nrs_tacq', 'nrs_taslit', 'nrs_taconfirm', 'nrs_confirm', 'nrs_image',
           'nrs_focus', 'nrs_mimf', 'nrs_bota']


def get_pipelines(exp_type):
    """Sorts which pipeline to use based on exp_type

    Parameters
    ----------
    exp_type: str
        JWST exposure type
    
    Returns
    -------
    pipeline: list
        Pipeline(s) to return for calibrating files.
    """

    if 'DARK' in exp_type:
        pipeline = [calwebb_dark.DarkPipeline()]
    elif 'FLAT' in exp_type:
        pipeline = [Detector1Pipeline()]
    elif exp_type.lower() in IMAGING:
        pipeline = [Detector1Pipeline(), calwebb_image2.Image2Pipeline()]
    else:
        pipeline = [Detector1Pipeline(), calwebb_spec2.Spec2Pipeline()]

    return pipeline


def override_reference_file(ref_file, pipeline):
    dm = datamodels.open(ref_file)
    for step in pipeline.step_defs.keys():
        # check if a step has an override_<reftype> option
        if hasattr(getattr(pipeline, step), 'override_{}'.format(dm.meta.reftype)):
            setattr(getattr(pipeline, step), 'override_{}'.format(dm.meta.reftype), ref_file)
            print('Setting {} in {} step'.format('override_{}'.format(dm.meta.reftype), step))

    return pipeline


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


def find_matches(ref_file, session, max_matches=-1):
    """Find matches in user provided database based on header keywords
    inside of user provided reference file.
    
    Parameters
    ----------
    ref_file: str
        File path to reference file to test
    session: sqlite session object
        A sqlite database session.
    max_matches: int
        Maximum matches to return. (Default=-1, return all matches)

    Returns
    -------
    matches: list
        a list of filenames
    """

    # Create a JWST datamodel based off of the reference file.
    dm = datamodels.open(ref_file)
    
    # Get the calibration context, pipeline map (pmap), instrument map (imap)
    # and reference map (rmap). For more detail on these maps, visit:
    # https://hst-crds.stsci.edu/static/users_guide/rmap_syntax.html
    context = crds.heavy_client.get_processing_mode('jwst')[1]
    pmap = crds.rmap.load_mapping(context)
    imap = pmap.get_imap(dm.meta.instrument.name)
    rmap = imap.get_rmap(dm.meta.reftype)
    
    meta_attrs = rmap.get_required_parkeys()
    meta_attrs.remove('META.OBSERVATION.DATE')
    meta_attrs.remove('META.OBSERVATION.TIME')

    query_args = []
    keys_used = []
    
    for attr in meta_attrs:
        
        # Hack to get around MIRI Dark Issue for now.
        if attr in ['META.EXPOSURE.READPATT', 'META.SUBARRAY.NAME']:
            continue

        if p_mapping[attr].lower() in dm.to_flat_dict():
            p_attr = p_mapping[attr]

            if '|' in dm[p_attr.lower()]:

                or_vals = dm[p_attr.lower()].split('|')[:-1]
                or_vals = [val.strip() for val in or_vals]
                query_args.append(or_(getattr(db.RegressionData, meta_to_fits[attr]) == val for val in or_vals))
                keys_used.append([meta_to_fits[attr], dm[p_attr.lower()]])

        # Ignore special CRDS-only values
        elif dm[attr.lower()] in ['GENERIC', 'N/A', 'ANY']:
            pass

        # Normal values
        else:
            query_args.append(getattr(db.RegressionData, meta_to_fits[attr]) == dm[attr.lower()])
            keys_used.append([meta_to_fits[attr], dm[attr.lower()]])

    query_string = '\n'.join(['\t{} = {}'.format(key[0], key[1]) for key in keys_used])
    print('Searching DB for test data with\n'+query_string)
    
    query_result = session.query(db.RegressionData).filter(*query_args)
    filenames = [os.path.join(result.path, result.filename) for result in query_result]
    
    print('Found {} instances:'.format(len(filenames)), end="")
    print('\n'+'\n'.join(['\t'+f for f in filenames]))
    
    if filenames:
        if max_matches > 0:
            print('Using first {} matches'.format(max_matches))
    else:
        print('\tNo matches found')

    return filenames[:max_matches]


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


def find_hst_matches(ref_file, session):
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
            print("REFERENCE FILE ISNT IN EXT 1 OR 2")

def calibrate_files(pipeline, file_loc, outdir):
    """Calibrate files with appropriate pipeline
    """

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

    files = find_hst_matches(ref_file, session)
    copy_files(files, ref_file, num_cpu)

    # if you only want to test one JWST file against ref file
    # else, search DB for files that will be effected by new ref file
    # if data_file is not None:
    #     file_to_cal = delayed(test_reference_file)(ref_file, data_file)
    #     tab_data = file_to_cal.compute()
    #     pd.set_option('display.max_colwidth', -1)
    #     print(pd.DataFrame(tab_data))
    # else:
    #     session = db.load_session(db_path=args['<db_path>'])
    #     if args['--max_matches']:
    #         data_files = find_matches(ref_file, session, max_matches=int(args['--max_matches']))
    #     else:
    #         data_files = find_matches(ref_file, session)
    #     # If files are returned, build list of objects to process
    #     if data_files:
    #         delayed_data_files = [delayed(test_reference_file)(ref_file, fname) 
    #                               for fname in data_files]
    #         # Check to make sure user isn't exceeding number of CPUs.
    #         if int(args['--num_cpu']) > psutil.cpu_count():
    #             args = (psutil.cpu_count(), args['--num_cpu'])
    #             err_str = "YOUR MACHINE ONLY HAS {} CPUs! YOU ENTERED {}"       
    #             raise ValueError(err_str.format(*args))
    #         else:
    #             # Compute results in parallel.
    #             print("Performing Calibration...")
    #             with ProgressBar():
    #                 tab_data = compute(delayed_data_files, num_workers=int(args['--num_cpu']))[0]
            
    #         # If you want to email, 
    #         if args['--email']:
    #             send_email(tab_data, args['--email'])
    #         else:
    #             pd.set_option('display.max_colwidth', -1)
    #             print(pd.DataFrame(tab_data))