Reference File Testing Tool
---------------------------

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge

.. image:: https://readthedocs.org/projects/reference-file-testing-tool/badge/?version=latest
    :target: http://reference-file-testing-tool.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

.. image:: https://travis-ci.org/STScI-MESA/reference-file-testing-tool.svg?branch=master
    :target: https://travis-ci.org/STScI-MESA/reference-file-testing-tool
    :alt: Build Status

Requirements
------------
The Reference File Testing Tool has the following dependencies:

- `Python <http://www.python.org/>`_ 3.5

- `SQLAlchemy <http://www.sqlalchemy.org/>`_

- `Astropy <http://http://www.astropy.org/>`_

- `JWST Calibration Pipeline <http://ssb.stsci.edu/doc/jwst_dev/>`_

- `Docopt <http://docopt.org>`_

- `DASK <http://dask.pydata.org/en/latest/>`_

- `Pandas <https://pandas.pydata.org>`_


Installing the Reference File Testing Tool in linux (*might not work in a Mac*)
-----------------------------------------------------------------------------

The Tool is currently in early development and must be installed from the GitHub development repository.

Building From Source
--------------------

The latest development version of the Tool can be cloned from GitHub using this command ::

    git clone git@github.com:spacetelescope/reference-file-testing-tool.git


To install the Tool (from the root of the source tree) ::

    python setup.py install

Outline for Basic Usage
-----------------------

1. Build a database that contains uncalibrated JWST files.

2. Query the database for files affected by reference file you wish to test.

3. Calibrate datasets returned from database with JWST pipeline and reference file you provide

4. Display results of testing to user in terminal or email.


Building and Populating the Database
------------------------------------

How do we create and add data to the database? Use the ``db_utils`` entry point. ::

    $ db_utils --help

    Usage:
        db_utils (create | remove) <db_path>
        db_utils (add | replace | force | full_reg_set | full_force) <db_path> <file_path> [--extension=<ext>] [--num_cpu=<n>]

    Arguments:
        <db_path>     Absolute path to database. 
        <file_path>   Absolute path to fits file to add. 

    Options:
         -h --help        Show this screen.
        --version         Show version.
        --num_cpu=<n>     number of cpus to use [default: 2]
        --extension=<ext>  extension [default: fits]

To create the database, we will use the ``create`` option. ::

    $ db_utils create /your/path/your_db_name.db

Now that we have a database, how do we store or manipulate data inside of it? We have a couple of options here... ::

    $ db_utils add /your/path/your_db_name.db /path/to/file/jwst_uncal.fits 

This will add a single row in the database for ``jwst_uncal.fits`` with the filename as the primary key for the table.

Lets say that there is an updated version of ``jwst_uncal.fits`` that you want to add to the database. 
If you try to use ``add`` the code will return an error because there is already an entry with the primary key ``jwst_uncal.fits``. 
To 'update' the row, we will use the ``replace`` option. ::

    $ db_utils replace /your/path/your_db_name.db /path/to/updated_file/jwst_uncal.fits 

Now the database contains the entry for the updated version of the file.

When adding files one at a time, you will only be able to add one file with a specific JWST observing mode. To override this functionality, use the
``force`` option. ::
    
    $ db_utils force /your/path/your_db_name.db /path/to/file/file_with_same_obsmode_as_jwst_uncal.fits 

Now you will have two different files with the same observing mode parameters from the headers in the database.

Adding Multiple Files at Once
-----------------------------
To add all the data from a top level directory downward. ``full_force`` uses python's `os.walk <https://docs.python.org/2/library/os.html#os.walk>`_
function to examine all directories for files within the root directory provided. ::

    $ db_utils full_force /your/path/your_db_name.db /path/to/dir/with/dirs_of_data

You will then be prompted by some messages printed to the screen along with the a progress bar. From a user perspective, this lets you know the code isn't
hung up. The ``[--num_cpu=<n>]`` by default is set to 2 by default but can be increased depending on the computing power of your machine. Entering ::

    $ db_utils full_force /your/path/your_db_name.db /path/to/dir/with/dirs_of_data --num_cpu=8

If your directory has different type of calibrated inputs and outputs and you only want to upload an specific type, you can use the option 
``[--extension=<ext>]``, by default it is set to --extension=fits


Adding All Unique Files at Once
-------------------------------

To add all the unique data from a top level directory downward. ``full_reg_set`` uses python's `os.walk <https://docs.python.org/2/library/os.html#os.walk>`_
function to examine all directories for files within the root directory provided. ::

    $ db_utils full_reg_set /your/path/your_db_name.db /path/to/dir/with/dirs_of_data

You will then be prompted by some messages printed to the screen along with the a progress bar. From a user perspective, this lets you know the code isn't
hung up. The ``[--num_cpu=<n>]`` by default is set to 2 by default but can be increased depending on the computing power of your machine. Entering ::

    $ db_utils full_reg_set /your/path/your_db_name.db /path/to/dir/with/dirs_of_data --num_cpu=8

will find, preprocess and ingest the data using 8 workers. The code performs a check for the number of cpus on your machine before executing
to make sure you aren't exceeding the number of cores you have available.  

Note that this option will only add the first dataset of a given mode will be added to the database.  If the added dataset is not the one you wanted,
you can use the option ``replace`` to get your favorite dataset in the db

This option only adds the _uncal.fits files to the db
    
Testing JWST Reference File
---------------------------

Now that we have a nicely populated database with all kinds of raw JWST data to test against, how do we perform the tests for a reference file? ::

    $ test_ref_file --help
    
    Script for testing reference files

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

To test your JWST reference file against a single uncalibrated JWST file, you won't need the database at all! Although the path to the database is required,
it is not used. ::

    $ test_ref_file /your/path/jwst_ref_file.fits /your/path/your_db_name.db --data=/path/to/single/jwst_raw_file.fits

This will calibrate your individual file with the reference file you provided. If you do not provide the ``--data`` command line arguement, the code
will use the database. By default, all files that are returned from database will be calibrated using the reference file you provide. ::

    $ test_ref_file /your/path/jwst_ref_file.fits /your/path/your_db_name.db

If you are only interested in calibrating a specific number of files when you query the database use the ``--max_matches`` arguement. ::

    $ test_ref_file /your/path/jwst_ref_file.fits /your/path/your_db_name.db --max_matches=20

Will only calibrate the first 20 results returned from the database. 

To speed things up, you can increase the number of workers by using the ``--num_cpu`` arguement (default is 2) ::

    $ test_ref_file /your/path/jwst_ref_file.fits /your/path/your_db_name.db --max_matches=20 --num_cpu=8

Will calibrate the first 20 results with 8 workers.

To get the results in a nicely formatted HTML table, use the ``--email`` arguement. ::

    $ test_ref_file /your/path/jwst_ref_file.fits /your/path/your_db_name.db --max_matches=20 --email username@stsci.edu

License
-------

This project is Copyright (c) Association of Universities for Research in Astronomy (AURA) and licensed under the terms of the BSD 3-Clause license. See the licenses folder for more information.
