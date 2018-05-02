************
Installation
************

Requirements
============
The Reference File Testing Tool has the following dependencies:

- `Python <http://www.python.org/>`_ 3.5

- `SQLAlchemy <http://www.sqlalchemy.org/>`_

- `Astropy <http://http://www.astropy.org/>`_

- `JWST Calibration Pipeline <http://ssb.stsci.edu/doc/jwst_dev/>`_

Installing the Reference File Testing Tool
==========================================

Using conda
-----------

To install the Tool with ``conda``, start with a JWST Calibration Pipeline environment, for example::

    conda create -n jwst-0.7.8rc2 --file http://ssb.stsci.edu/releases/jwstdp/0.7.8/dev/jwstdp-0.7.8rc2-osx-py27.0.txt

Then install the tool from the ``stsci-mesa`` channel::

    source activate jwst-0.7.8rc2
    conda install --channel stsci-mesa reftest

Building from source
--------------------

The latest development version of the Tool can be cloned from GitHub using this command::

    git clone git://github.com/STScI-MESA/reference-file-testing-tool.git
    
To update the tool you can do within your sandbox::
    
    git pull

To install the Tool (from the root of the source tree)::

    python setup.py install

