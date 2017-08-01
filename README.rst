Reference File Testing Tool
---------------------------

.. image:: http://img.shields.io/badge/powered%20by-AstroPy-orange.svg?style=flat
    :target: http://www.astropy.org
    :alt: Powered by Astropy Badge

.. image:: https://readthedocs.org/projects/reference-file-testing-tool/badge/?version=latest
    :target: http://reference-file-testing-tool.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

Requirements
------------
The Reference File Testing Tool has the following dependencies:

- `Python <http://www.python.org/>`_ 2.7

- `SQLAlchemy <http://www.sqlalchemy.org/>`_

- `Astropy <http://http://www.astropy.org/>`_

- `JWST Calibration Pipeline <http://ssb.stsci.edu/doc/jwst_dev/>`_

Installing the Reference File Testing Tool
------------------------------------------

The Tool is currently in early development and must be installed from the GitHub development repository.

Using pip
---------

To install the Tool with `pip <http://www.pip-installer.org/en/latest/>`_, simply run::

    pip install git+git://github.com/STScI-MESA/reference-file-testing-tool.git

Building from source
--------------------

The latest development version of the Tool can be cloned from GitHub using this command::

    git clone git://github.com/STScI-MESA/reference-file-testing-tool.git

To install the Tool (from the root of the source tree)::

    python setup.py install

Basic usage
-----------

The basic component of the Reference File Testing Tool is the ``test_reference_file`` function.  After
installing the package it can be called from the command line with::

    test_reference_file /path/to/my/reference_file --data /path/to/some_uncal.fits

where the ``--data`` argument is some suitable level 1b JWST data.  This will run the JWST calibration pipeline on the
uncalibrated data overriding the default reference file with the one supplied.


License
-------

This project is Copyright (c) Matthew Hill and licensed under the terms of the BSD 3-Clause license. See the licenses folder for more information.
