from __future__ import print_function
from . import db

import os
from jwst.pipeline import calwebb_dark, calwebb_image2, calwebb_spec2

try:
    from jwst.pipeline import SloperPipeline as Detector1Pipeline
except ImportError:
    from jwst.pipeline import Detector1Pipeline

from jwst import datamodels
import crds
from astropy.io import fits
import logging
from sqlalchemy import or_
try:
    from cStringIO import StringIO      # Python 2
except ImportError:
    from io import StringIO

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
    "META.EXPOSURE.READPATT": "META.EXPOSURE.P_READPATT",
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
           'nis_image', 'nis_ami', 'nis_tacq', 'nis_taconfirm', 'nis_focus',
           'nrs_tacq', 'nrs_taslit', 'nrs_taconfirm', 'nrs_confirm', 'nrs_image',
           'nrs_focus', 'nrs_mimf', 'nrs_bota']

def get_pipelines(exp_type):
    if 'DARK' in exp_type:
        return [calwebb_dark.DarkPipeline()]
    elif 'FLAT' in exp_type:
        return [Detector1Pipeline()]
    elif exp_type.lower() in IMAGING:
        return [Detector1Pipeline(), calwebb_image2.Image2Pipeline()]
    else:
        return [Detector1Pipeline(), calwebb_spec2.Spec2Pipeline()]

def override_reference_file(ref_file, pipeline):
    dm = datamodels.open(ref_file)
    for step in pipeline.step_defs.keys():
        # check if a step has an override_<reftype> option
        if hasattr(getattr(pipeline, step), 'override_{}'.format(dm.meta.reftype)):
            setattr(getattr(pipeline, step), 'override_{}'.format(dm.meta.reftype), ref_file)
            print('Setting {} in {} step'.format('override_{}'.format(dm.meta.reftype), step))

    return pipeline

def test_reference_file(ref_file, data_file):
    """
    Override CRDS reference file with the supplied reference file and run
    pipeline with supplied data file.
    
    Parameters
    ----------
    ref_file: str
        Path to reference file.
    data_file: str
        Path to data file.
    """

    # redirect pipeline log from sys.stderr to a string
    log_stream = StringIO()
    stpipe_log = logging.Logger.manager.loggerDict['stpipe']
    stpipe_log.handlers[0].stream = log_stream

    # allow invalid keyword values
    os.environ['PASS_INVALID_VALUES'] = '1'

    print('Testing {}'.format(data_file))
    result = data_file
    try:
        for pipeline in get_pipelines(fits.getheader(data_file)['EXP_TYPE']):
            pipeline = override_reference_file(ref_file, pipeline)
            print('Running {}'.format(pipeline))
            result = pipeline.run(result)
            print('Completed {}'.format(pipeline))

        return 1

    except Exception as err:
        print('{} failed with error {}'.format(data_file, (err.message)))
        return 0


def find_matches(ref_file, session, max_matches=None):
    """
    
    Parameters
    ----------
    ref_file: str
        File path to reference file to test
    max_matches: int
        Maximum matches to return. (Default=-1, return all matches)

    Returns
    -------
    matches: list
        a list of filenames

    """
    dm = datamodels.open(ref_file)
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

        # Deal with OR values
        if p_mapping[attr].lower() in dm.to_flat_dict():
            p_attr = p_mapping[attr]

            if '|' in dm[p_attr.lower()]:

                or_vals = dm[p_attr.lower()].split('|')[:-1]
                or_vals = [val.strip() for val in or_vals]
                query_args.append(or_(getattr(db.TestData, meta_to_fits[attr]) == val for val in or_vals))
                keys_used.append([meta_to_fits[attr], dm[p_attr.lower()]])

        # Ignore special CRDS-only values
        elif dm[attr.lower()] in ['GENERIC', 'N/A', 'ANY']:
            pass

        # Normal values
        else:
            query_args.append(getattr(db.TestData, meta_to_fits[attr]) == dm[attr.lower()])
            keys_used.append([meta_to_fits[attr], dm[attr.lower()]])

    query_string = '\n'.join(['\t{} = {}'.format(key[0], key[1]) for key in keys_used])
    print('Searching DB for test data with\n'+query_string)
    query_result = session.query(db.TestData).filter(*query_args)
    filenames = [result.filename for result in query_result]
    print('Found {} instances:'.format(len(filenames)), end="")
    print('\n'+'\n'.join(['\t'+f for f in filenames]))
    if filenames:
        if max_matches > 0:
            print('Using first {} matches'.format(max_matches))
    else:
        print('\tNo matches found')

    return filenames[:max_matches]

