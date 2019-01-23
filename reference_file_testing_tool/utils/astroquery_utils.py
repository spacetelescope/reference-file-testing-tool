#! /usr/bin/env python

import os
import grp
import stat
import shutil
import glob
from tempfile import mkdtemp
from astroquery.mast import Mast, Observations
from .version import __author__, __version__

# UNIX_GRP = 'stisgo'  # for proprietary data
OUTDIR = './'
RAW_TYPES = ['RAW', 'WAV', 'WSP', 'SPT', 'EPC', 'TAG', 'ASN', 'JIF', 'JIT', 'TRL']

def getdata(rootnames, guest=False, outdir=OUTDIR):
    ''' Wrapper to `astroquery.mast` interface for downloading HST data.
    Based off of STIS tool written by S. Lockwood.
    
    Parameters:
        rootnames : str
            Observation rootnames to retrieve
        guest : bool, optional
            Use MAST account username `guest` for anonymous public access?  [default=False]
            Otherwise, the token stored in $MAST_API_TOKEN is used.  This may be set at 
            https://auth.mast.stsci.edu
        outdir : str, optional
            Location to send data [default='./']
    '''
    
    tmp_dir = mkdtemp(prefix='tmp_download_data_')
    print ('TMP_DIR:  {}'.format(tmp_dir))
    try:
        # Login to access MAST data:
        obs = Observations()
        if not guest:
            obs.login()
        
        # Determine data to download:
        t = obs.query_criteria(obs_id=rootnames, obstype='all')
        proprietary = {x['obs_id'][:-1].lower() : x['dataRights'] != 'PUBLIC' for x in t}
        product_list = obs.get_product_list(t)
        
        # Download data:
        downloads = obs.download_products(product_list, 
            productSubGroupDescription=RAW_TYPES, 
            extension='fits', 
            mrp_only=False, 
            download_dir=tmp_dir, 
            curl_flag=false)
        
        # Print download status:
        for x in downloads:
            print ('{} {}:\t{}'.format(os.path.basename(x['Local Path']), x['Status'], x['Message']))
        
        # Find and copy data from temporary directory to specified location:
        files = glob.glob(os.path.join(tmp_dir, 'mastDownload', 'HST', '*', '*.fits')) + \
            glob.glob(os.path.join(tmp_dir, '*.sh'))
        for f in files:
            outname = os.path.join(outdir, os.path.basename(f))
            shutil.copy(f, outname)
            
            if os.path.basename(f)[-3:] == '.sh':
                os.chmod(outname, stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP)  # -rwxrwx---
            elif proprietary.get(os.path.basename(f).split('_',1)[0][:-1].lower(), True):
                # Set Unix permissions for proprietary and unknown datasets:
                os.chown(outname, os.getuid(), grp.getgrnam(UNIX_GRP).gr_gid)
                os.chmod(outname, stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)  # -rw-rw----
    finally:
        shutil.rmtree(tmp_dir)