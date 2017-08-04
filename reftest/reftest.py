from . import db
# from . import log

from jwst.pipeline import calwebb_sloper, calwebb_image2, calwebb_spec2
import crds
from astropy.io import fits
# import logging
from sqlalchemy import or_

# log = logging.getLogger(__name__)
# log.setLevel(logging.DEBUG)

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

IMAGING = ['fgs_image', 'fgs_focus', 'fgs_skyflat', 'fgs_intflat', 'mir_image', 'mir_tacq', 'mir_lyot', 'mir_4qpm', 'mir_coroncal', 'nrc_image',
'nrc_tacq', 'nrc_coron', 'nrc_taconfirm', 'nrc_focus', 'nrc_tsimage', 'nis_image', 'nis_ami', 'nis_tacq', 'nis_taconfirm', 'nis_focus', 'nrs_tacq', 'nrs_taslit',
'nrs_taconfirm', 'nrs_confirm', 'nrs_image', 'nrs_focus', 'nrs_mimf', 'nrs_bota']

def get_level2b_pipeline(exp_type):
    if exp_type.lower() in IMAGING:
        return calwebb_image2.Image2Pipeline()
    else:
        return calwebb_spec2.Spec2Pipeline()

def override_reference_file(ref_file, pipeline):
    header = fits.getheader(ref_file)
    for step in pipeline.step_defs.keys():
        # check if a step has an override_<reftype> option
        if hasattr(getattr(pipeline, step), 'override_{}'.format(header['REFTYPE'].lower())):
            setattr(getattr(pipeline, step), 'override_{}'.format(header['REFTYPE'].lower()), ref_file)
            print('Setting {} in {} step'.format('override_{}'.format(header['REFTYPE'].lower()), step))

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
    level2a_pipeline = calwebb_sloper.SloperPipeline()
    level2b_pipeline = get_level2b_pipeline(fits.getheader(data_file)['EXP_TYPE'])

    level2a_pipeline = override_reference_file(ref_file, level2a_pipeline)
    level2b_pipeline = override_reference_file(ref_file, level2b_pipeline)
    print('Testing {}'.format(data_file))
    try:
        print('Running {}'.format(level2a_pipeline))
        result = level2a_pipeline.run(data_file)
        print('Completed {}'.format(level2a_pipeline))
        print('Running {}'.format(level2b_pipeline))
        level2b_pipeline.run(result)
        print('Completed {}'.format(level2b_pipeline))

    except Exception as err:
        print('{} failed with error {}'.format(data_file, (err.message)))


def find_matches(ref_file, session, max_matches=-1):
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
    header = fits.getheader(ref_file)
    context = crds.heavy_client.get_processing_mode('jwst')[1]
    pmap = crds.rmap.load_mapping(context)
    imap = pmap.get_imap(header['INSTRUME'])
    rmap = imap.get_rmap(header['REFTYPE'])
    meta_attrs = rmap.get_required_parkeys()
    meta_attrs.remove('META.OBSERVATION.DATE')
    meta_attrs.remove('META.OBSERVATION.TIME')

    query_args = []
    keys_used = []
    for attr in meta_attrs:
        # Ignore special CRDS-only values
        if header[meta_to_fits[attr]] in ['GENERIC', 'N/A', 'ANY']:
            pass

        # Deal with OR values
        elif '|' in header[meta_to_fits[attr]]:
            or_vals = header[meta_to_fits[attr]].split('|')
            query_args.append(or_(getattr(db.TestData, meta_to_fits[attr]) == val for val in or_vals))
            keys_used.append(meta_to_fits[attr])

        # Normal values
        else:
            query_args.append(getattr(db.TestData, meta_to_fits[attr]) == header[meta_to_fits[attr]])
            keys_used.append(meta_to_fits[attr])

    query_string = '\n'.join(['\t{} = {}'.format(key, header[key]) for key in keys_used])
    print('Searching DB for test data with\n'+query_string)
    query_result = session.query(db.TestData).filter(*query_args)
    filenames = [result.filename for result in query_result]
    print('Found {} instances:\n'.format(len(filenames))+'\n'.join(['\t'+f for f in filenames]))
    if max_matches > 0:
        print('Using first {} matches'.format(max_matches))
    return filenames[:max_matches]

