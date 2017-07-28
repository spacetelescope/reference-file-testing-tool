from . import db

from jwst.pipeline import calwebb_sloper, calwebb_image2, calwebb_spec2
import crds
from astropy.io import fits

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

    header = fits.getheader(ref_file)
    ref_type = header['REFTYPE']
    sloper_pipeline = calwebb_sloper.SloperPipeline()
    for step in sloper_pipeline.step_defs.keys():
        # check if a step has an override_<reftype> option
        if hasattr(getattr(sloper_pipeline, step), 'override_{}'.format(header['REFTYPE'].lower())):
            setattr(getattr(sloper_pipeline, step), 'override_{}'.format(header['REFTYPE'].lower()), ref_file)

    sloper_pipeline.run(data_file)

def find_matches(header):
    context = crds.heavy_client.get_processing_mode('jwst')[1]
    pmap = crds.rmap.load_mapping(context)
    imap = pmap.get_imap(header['INSTRUME'])
    rmap = imap.get_rmap(header['REFTYPE'])
    meta_attrs = rmap.get_required_parkeys()
    meta_attrs.remove('META.OBSERVATION.DATE')
    meta_attrs.remove('META.OBSERVATION.TIME')

    query_args = {}
    for attr in meta_attrs:
        query_args[meta_to_fits[attr]] = header[meta_to_fits[attr]]


    session = db.load_session(db.REFTEST_DATA_DB)
    query_result = session.query(db.TestData).filter_by(**query_args)
    return [result.filename for result in query_result]


def main(args=None):

    from astropy.utils.compat import argparse

    parser = argparse.ArgumentParser(
        description="Check that a reference file runs in the calibration pipeline",
        )
    parser.add_argument('reference_file', help='the reference file to test')
    parser.add_argument('--data', help='data to run pipeline with', default=None)

    res = parser.parse_args(args)

    ref_file = res.reference_file
    data_file = res.data

    if data_file is not None:
        test_reference_file(ref_file, data_file)
    else:
        print('A data file is currently required to run')
