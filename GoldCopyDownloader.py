# -*- coding: utf-8 -*-
"""
Created on Mon Jul 20 13:50:04 2020
2020-07-20-lg this will download the needed files from S3
@author: SMD7
"""

import glob
import datetime
import pandas as pd
from pathlib import Path
from pathlib import PurePosixPath
import pyarrow.parquet as pq
# =============================================================================
# 
# =============================================================================
basefiledir='j://LinkUp//'#place your drive letter here/or drive and base path
accesskeyid='xxx'#replace xxx with your key id
accesskeysecretid='xxxx'#replace xxxx with your secret access key
getaggregates=[]#reserved for getting specific aggregates
#select files you want - 1 or 0
getdict={'jobs_base':1,
         'jobs_log':1,
         'scrapelog':1,
         'descriptions':1,
         'reference':1,
         'advancedAnalytics':1,
         'auxiliary':1,
         'aggregates':0#coming soon
         }
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
# # # # You should not need to go below here.  
# =============================================================================
# =============================================================================
# =============================================================================
# =============================================================================
py='GoldCopyDownloader.py'
runtime=datetime.datetime.utcnow()
bucket='smd-lu'
# =============================================================================
# #function to get all available files
# =============================================================================
def GetS3BucketObjectList(bucket,accesskeyid,accesskeysecretid):
    import boto3
    from boto.s3.connection import S3Connection
    import dateutil.parser as dparser
    from dateparser.search import search_dates
    
    try:
        s3 = boto3.resource('s3')
        # Connect to S3    
        c=S3Connection(accesskeyid,accesskeysecretid)
        b=c.get_bucket(bucket)
        filelist=[]
        for key in b.list():
            filelist.append(key.name)
            print(key.name)
        filedf=pd.DataFrame(filelist)
        filedf.columns=['fileloc']
        filedf['filename']=filedf.fileloc.str.rsplit('/',1).str[1]
        rflg=1
        msg=bucket+' File Names retrieved from S3'
    except:
        rflg=0
        msg=bucket+' List from S3 FAILED in'
        filedf=''
    return rflg,msg,filedf
# =============================================================================
# #function to get the file from S3, makes directory if needed
# =============================================================================
def GetFileFromS3_all(localfile,keyname,bucket,accesskeyid,accesskeysecretid):
    from boto.s3.connection import S3Connection
    import os
    from pathlib import Path
    #checkiffilegotalready  ???
    try:  
        # Connect to S3    
        c=S3Connection(accesskeyid,accesskeysecretid)
        b=c.get_bucket(bucket)
        print ('Got Connected')
        key=b.get_key(keyname, validate=True)
        size = key.size
        if os.path.isfile(localfile):
            msg=localfile+' Already Exists - skipping to next'
            print(msg)
            rflg=1
        else:
            localfiledir=str(Path(localfile).parent)
            if os.path.isdir(localfiledir):
                pass
            else:
                try:
                    os.mkdir(localfiledir)
                except:
                    os.makedirs(localfiledir, exist_ok=True)
            key.get_contents_to_filename(localfile,version_id=None)
            filesize=os.path.getsize(localfile) 
            if filesize==size:
                msg=localfile+' is size = '+str(size)
                rflg=1
            else:
                msg=localfile+' SIZE IS WRONG'
                rflg=0
    except:
        msg=localfile+' FAILED TO DOWNLOAD'
        rflg=0
    return msg,rflg
# =============================================================================
# end get file from s3
# =============================================================================
     
excludes={'reference':{'SMD':['reftype=CIK','reftype=ManualCheckFlag',
                 'reftype=MatchScore','reftype=MatchScoreFlag',
                 'reftype=ticker_ric','reftype=TRBC_BS',
                 'reftype=TRBC_ES','reftype=TRBC_I',
                 'reftype=TRBC_IG','reftype=URL']}}


for k,v in getdict.items():
    if v==1:
        getdict[k]=['',str(Path(basefiledir+k))]
        
#scan smartmarketdata for all linkup files
totalfilescnt=0
#get all the files on S3
rflg,msg,filedf=GetS3BucketObjectList(bucket,accesskeyid,accesskeysecretid)
#clean out excludes
for k,v in excludes.items():
    for k2,v2 in v.items():
        for x in v2:
            filedf=filedf.loc[~filedf.fileloc.str.contains(x),:]
for fb,v in getdict.items():
    #get list of all files for this filebase that are on S3
    tmpfiledf=filedf.loc[filedf.fileloc.str.contains(fb),:].copy()
    tmpfiledf=tmpfiledf.loc[tmpfiledf.fileloc.str.contains('parquet'),:]
    tmpfiledf['localfile']=tmpfiledf.fileloc.apply(lambda x:str(Path(v[1],x.split('/',1)[1])))
    #getlist of files already have locally
    basefilechilddir=str(Path(basefiledir))
    getdict[fb][0]=pd.DataFrame(glob.glob(str(basefilechilddir)+'\\**',recursive=True),columns=['fileloc'])
    getdict[fb][0]=getdict[fb][0].loc[getdict[fb][0].fileloc.str.contains('parquet'),:]
    totalfileslen=len(getdict[fb][0])
    totalfilescnt=totalfilescnt+totalfileslen
    msg=fb+' '+str(totalfileslen)+' files local'
    print(msg)
    #whittledown
    tmpfiledf=tmpfiledf.loc[~tmpfiledf.localfile.isin(getdict[fb][0].fileloc),:]
    for i,r in tmpfiledf.iterrows():
        localfile=r.localfile
        keyname=r.fileloc
        msg,rflg=GetFileFromS3_all(localfile,keyname,bucket,accesskeyid,accesskeysecretid)
        print(msg)
# =============================================================================
# END        
# =============================================================================
# =============================================================================
#     #example ingest parquet into dataframe
#     import pyarrow.parquet as pq     
#     #test=pq.ParquetDataset(filebases[fb][1]+'\\soccode\\soc_2010')#, filters=[('created_pit', '<=', str(gdate)),])
#     test=pq.ParquetDataset(filelocation)#, filters=[('created_pit', '<=', str(gdate)),])
#     test=test.read(use_pandas_metadata=True)
#     test=test.to_pandas()
#     
#   
#     
#         
# =============================================================================
                
   
    