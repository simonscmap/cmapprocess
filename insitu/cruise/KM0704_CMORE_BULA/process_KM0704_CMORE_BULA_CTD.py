import pandas as pd
import glob
import os
import re
from cmapingest import vault_structure as vs


"""CTD HEADER
  CTDPRS  CTDTMP  CTDSAL  CTDOXY     PAR  LS6000  CHLPIG NITRATE  NUMBER    QUALT1
    DBAR  ITS-90  PSS-78 uMOL/KG   Volts   Volts    uG/L uMOL/KG    OBS.         *

    CMORE-BULA CTD data was collected using a SeaBird CTD 9-11 Plus at
the maximum sampling rate of 24 samples per second (24 Hz). They were
screened for errors and processed to 1-dbar averages. Each file
contains one full profile (e.g., bu1s2c1dn.ctd contains the downcast CTD
data from CMORE-BULA 1, station 2, cast 1).


"""

""" Bottle
	Column  Format	Item
	 1-  8    i3	Station Number
	 9- 16    i3	Cast Number
	17- 24    i3	Rossette Position
	25- 32  f7.3	Latitude [Degrees North]
	33- 40  f7.3	Longitude [Degrees West] 


"""


"""underway (uw)
	Column  Format	Item
	 1-  4	   i4	Year
	 6-  8	   i3	Julian day [GMT]
	10- 11	   i2	Hour [GMT]
	13- 14	   i2	Minute [GMT]
	16- 25  f10.6	Latitude [Degrees North]
	27- 37 	f11.6	Longitude [Degrees West]
        39- 46   f8.5	Salinity [PSS-78]
        48- 55   f8.5	Temperature [ITS-90]
        57- 64   f8.4	Chloropigment [ug/l]
        67- 69     a3	Quality flags for salinity - chloropigment

uwsample.txt:
1.	Underway Sample #
2.	Latitude [deg N]
3.	Longitude [deg W]
4.	UTC (Julian) day
5.	GMT
6.	Potential Density (Sigma) [kg/m3]
        - calculated using the thermosalinograph data

7.	Bottle Dissolved Oxygen [umol/l]
var_n...
"""

CMORE_BULA_path = vs.collected_data + "insitu/cruise/misc_cruise/KM0704_CMORE_BULA/"


def process_add_cols_ctd():
    """concat CTD, build station/cast/direction out of filename, zero padd number"""
    ctd_concat_list = []
    ctd_flist = glob.glob(CMORE_BULA_path + "CTD/" + "*.ctd")
    for ctd_cast in ctd_flist:
        df = pd.read_csv(
            ctd_cast,
            delim_whitespace=True,
            skiprows=3,
            names=[
                "CTDPRS",
                "CTDTMP",
                "CTDSAL",
                "CTDOXY",
                "PAR",
                "LS6000",
                "CHLPIG",
                "NITRATE",
                "NUMBER",
                "QUALT1",
            ],
        )
        station_cast_meta = os.path.basename(ctd_cast).split(".")[0].split("bu1")[1]
        df["station"] = station_cast_meta.split("s")[1].split("c")[0].zfill(3)
        df["cast"] = (
            station_cast_meta.split("c")[1].split("up")[0].split("dn")[0].zfill(3)
        )
        df["cast_direction"] = station_cast_meta[-2:]
        df["NUMBER"] = df["NUMBER"].astype(str).str.zfill(5)
        ctd_concat_list.append(df)
    concat_df = pd.concat(ctd_concat_list, axis=0, ignore_index=True)
    return concat_df


def process_underway():
    """create header, split flags, create time"""
    uway_path = CMORE_BULA_path + "underway/bu1uw2.dat"
    df = pd.read_csv(
        uway_path,
        delim_whitespace=True,
        names=[
            "year",
            "doy",
            "hour",
            "minute",
            "lat",
            "lon",
            "salinity",
            "temperature",
            "chloropigment",
            "quality_flags",
        ],
    )
    # builds time out of year, doy,hour,min
    df["time"] = pd.to_datetime(
        (df["year"]).astype(str)
        + (df["doy"]).astype(str)
        + (df["hour"].astype(str))
        + (df["minute"].astype(str)),
        format="%Y%j%H%M",
    )
    # split flags into variable flag columns
    df["salinity_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [1]]
    )
    df["temperature_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [2]]
    )
    df["chloropigment_flag"] = (
        df["quality_flags"].astype(str).str.split("", expand=True).iloc[:, [3]]
    )
    # removes old cols and reorders
    df.drop(["year", "doy", "hour", "minute", "quality_flags"], axis=1, inplace=True)
    df = df[
        [
            "time",
            "lat",
            "lon",
            "salinity",
            "temperature",
            "chloropigment",
            "salinity_flag",
            "temperature_flag",
            "chloropigment_flag",
        ]
    ]
    return df


def process_underway_sample():
    uway_sample_path = CMORE_BULA_path + "underway/bu1uwsamp.txt"
    df = pd.read_csv(
        uway_sample_path,
        delim_whitespace=True,
        skiprows=3,
        names=[
            "sample",
            "lat",
            "lon",
            "doy",
            "GMT",
            "sigma",
            "oxygen",
            "DIC",
            "alkalin",
            "phspht",
            "NO2_NO3",
            "silcat",
            "DOP",
            "DON",
            "DOC",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chlda",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
            "CH4",
            "N2O",
        ],
    )
    df["sample"] = df["sample"].astype(str).str.zfill(3)
    # convert datetime ## !!!ASSUMING YEAR IS 2007 BASED ON OTHER UNDERWAY!!!!
    df["year"] = "2007"
    df["hour"] = df["GMT"].astype(str).str.zfill(4)
    df["time"] = pd.to_datetime(
        (df["year"]).astype(str) + (df["doy"]).astype(str) + (df["hour"].astype(str)),
        format="%Y%j%H%M",
    )
    # drop old cols and reorder
    df.drop(["year", "doy", "hour"], axis=1, inplace=True)
    df = df[
        [
            "time",
            "lat",
            "lon",
            "sample",
            "sigma",
            "oxygen",
            "DIC",
            "alkalin",
            "phspht",
            "NO2_NO3",
            "silcat",
            "DOP",
            "DON",
            "DOC",
            "LLN",
            "LLP",
            "PC",
            "PN",
            "PP",
            "PSi",
            "chl_a",
            "pheo",
            "chlda",
            "chl_plus",
            "PERID",
            "19_but",
            "FUCO",
            "19_Hex",
            "Prasino",
            "Viol",
            "Diadino",
            "Allox",
            "Lutein",
            "Zeaxan",
            "Chl_b",
            "alpha_carotene",
            "beta_carotene",
            "divinyl_chl",
            "monovinyl_chl",
            "HPLC_chla",
            "hetero_bact",
            "prochloro_bact",
            "synecho_bact",
            "eukaryotes",
            "ATP",
            "CH4",
            "N2O",
        ]
    ]
    return df
