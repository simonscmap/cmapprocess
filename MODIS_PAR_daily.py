from tqdm import tqdm
import pandas as pd
import numpy as np
import dropbox
import xarray as xr
import glob
import os

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data
from cmapingest import transfer
from cmapingest import stats


startdate = "2010001" #YYYYDDD
# startdate = "2019185" #YYYYDDD

enddate = "2014365" #YYYYDDD


modis_par_path = vs.collected_data + "MODIS_PAR_daily_data/"
modis_flist = glob.glob(modis_par_path + "*.nc")
modis_flist_base = [os.path.basename(filename) for filename in modis_flist]
modis_file_date_list  = np.sort([i.strip('.L3m_DAY_PAR_par_9km.nc') for i in modis_flist_base])

files_in_range = []
for i in modis_flist_base:
    strpted_time = i.replace('.L3m_DAY_PAR_par_9km.nc','').replace('A','')
    zfill_time = strpted_time[:4] + strpted_time[4:].zfill(3)
    fdate = pd.to_datetime(zfill_time,format='%Y%j')
    if pd.to_datetime(startdate,format='%Y%j') <= fdate <=pd.to_datetime(enddate,format='%Y%j'):
        files_in_range.append(i)




import time

start = time.time()


vs.makedir(vs.satellite + 'tblModis_PAR')
for fil in tqdm(files_in_range):
    print(fil)
    end1 = time.time()
    print('1',end1 - start)
    timecol =  pd.to_datetime(fil.replace('.L3m_DAY_PAR_par_9km.nc','').replace('A',''),format='%Y%j').strftime('%Y-%m-%d')
    end2 = time.time()
    print('2',end2 - end1)
    xdf = xr.open_dataset(modis_par_path + fil)
    end3 = time.time()
    print('3',end3 - end2)
    df  = data.netcdf4_to_pandas(modis_par_path + fil,'par')
    df = df.head(100000)
    end4 = time.time()
    print('4',end4 - end3)

    df["time"] =  timecol
    df['year'] = pd.to_datetime(df['time']).dt.year
    df['month'] = pd.to_datetime(df['time']).dt.month
    df['week'] = pd.to_datetime(df['time']).dt.weekofyear
    df['dayofyear'] = pd.to_datetime(df['time']).dt.dayofyear

    df = data.clean_data_df(df)
    end5 = time.time()
    print('5',end5 - end4)
    df = df[['time','lat','lon','par']]

    df.to_hdf(vs.dbx_download_transfer +fil.replace('.nc','.h5'),key='tblModis_PAR')
    branch_path = cmn.vault_struct_retrieval('satellite')

    transfer.dropbox_file_transfer(vs.dbx_download_transfer +fil.replace('.nc','.h5'),dbx_vault +vs.vault.split('/vault/')[1] + branch_path + tableName + '/rep/' + fil.replace('.nc','.h5'))
    os.remove(vs.dbx_download_transfer +fil.replace('.nc','.h5'))

    end6 = time.time()
    print('6',end6 - end5)   
    stats.buildLarge_Stats(df,timecol,'tblModis_PAR','satellite',transfer='dropbox')
    end7 = time.time()
    print('7',end7 - end6)
    break
""" every file:
    1. import into xarray
    2. flatten to pandas df
    3. clean pandas df
    4. export pandas to /vault"""