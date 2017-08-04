"""
Module defining entry points for command line interface
"""

from . import reftest
from . import db

import argparse

def test_reference_file(args=None):
    parser = argparse.ArgumentParser(
        description="Check that a reference file runs in the calibration pipeline",
        )
    parser.add_argument('reference_file', help='the reference file to test')
    parser.add_argument('--data', help='data to run pipeline with', default=None)
    parser.add_argument('--max-matches', help='maximum number of data sets to test', default=None)
    parser.add_argument('--db-path', help='path to database of test data', default=None)

    res = parser.parse_args(args)

    ref_file = res.reference_file
    data_file = res.data
    success = 0
    if data_file is not None:
        success += reftest.test_reference_file(ref_file, data_file)
    else:
        session = db.load_session(db_path=res.db_path)
        if session is None:
            return
        data_files = reftest.find_matches(ref_file, session, max_matches=res.max_matches)
        if data_files:
            for data_file in data_files:
                success += reftest.test_reference_file(ref_file, data_file)
            print('Tests successful for {}/{} files'.format(success, len(data_files)))

    return success


def create_test_data_db(args=None):
    parser = argparse.ArgumentParser(
        description="Create the SQLite DB for test data",
        )
    parser.add_argument('db_path', help='the reference file to test')

    res = parser.parse_args(args)
    db.create_test_data_db(res.db_path)

def add_test_data(args=None):
    parser = argparse.ArgumentParser(
        description="Add files to the test data database."
    )
    parser.add_argument('file_path', help='Globable file string for test data')
    parser.add_argument('--force', help='Add a file to the database even if a similar one already exists', default=False)
    parser.add_argument('--db-path', help='Database to add file to', default=None)
    res = parser.parse_args(args)
    db.add_test_data(file_path=res.file_path, db_path=res.db_path, force=res.force)
