from tqdm import tqdm
import pandas as pd
import numpy as np
import vaex
import h5py 
import dask
import xarray as xr
import glob
import os

from cmapingest import vault_structure as vs
from cmapingest import common as cmn
from cmapingest import data

startdate = "2002185" #YYYYDDD
# startdate = "2019185" #YYYYDDD

enddate = "2019353" #YYYYDDD

modis_poc_path = vs.collected_data + "MODIS_POC_8_day_data/"
modis_flist = glob.glob(modis_poc_path + "*.nc")
modis_flist_base = [os.path.basename(filename) for filename in modis_flist]
modis_file_date_list  = np.sort([i.strip('.nc') for i in [i.strip('MODIS_POC_8_day_') for i in modis_flist_base]])
modis_file_date_DT  = [pd.to_datetime(i,format='%Y%j') for i in modis_file_date_list]

dates_in_range = [i for i in modis_file_date_DT if pd.to_datetime(startdate,format='%Y%j') < i <pd.to_datetime(enddate,format='%Y%j')]


files_in_range = []
for i in modis_flist_base:
    fdate = pd.to_datetime(i.strip('MODIS_POC_8_day_').strip('.nc'),format='%Y%j')
    if pd.to_datetime(startdate,format='%Y%j') < fdate <pd.to_datetime(enddate,format='%Y%j'):
        files_in_range.append(i)

vs.leafStruc(vs.satellite + 'tblMODIS_POC')
for fil in tqdm(files_in_range):
    print(fil)
    timecol =  pd.to_datetime(i.strip('MODIS_POC_8_day_').strip('.nc'),format='%Y%j').strftime('%Y-%m-%d')
    xdf = xr.open_dataset(modis_poc_path + fil)
    df  = data.netcdf4_to_pandas(modis_poc_path + fil,'poc')
    df["time"] =  pd.to_datetime(fil.strip('MODIS_POC_8_day_').strip('.nc'),format='%Y%j').strftime('%Y-%m-%d')
    df = data.clean_data_df(df)
    df = df[['time','lat','lon','poc']]
    df.to_csv(vs.satellite + 'tblMODIS_POC/rep/' +fil.strip('.nc') + '.csv',sep=',',index=False)

""" every file:
    1. import into xarray
    2. flatten to pandas df
    3. clean pandas df
    4. export pandas to /vault"""