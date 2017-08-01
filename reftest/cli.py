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
    parser.add_argument('--max-matches', type=int, help='maximum number of data sets to test', default=-1)

    res = parser.parse_args(args)

    ref_file = res.reference_file
    data_file = res.data

    if data_file is not None:
        reftest.test_reference_file(ref_file, data_file)
    else:
        data_files = reftest.find_matches(ref_file, res.max_matches)
        if data_files:
            for data_file in data_files:
                reftest.test_reference_file(ref_file, data_file)

        else:
            print('No matching test data found in database')


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
    parser.add_argument('--db', help='Database to add file to')
