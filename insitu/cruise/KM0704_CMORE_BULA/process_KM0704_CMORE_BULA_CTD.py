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


Bottle:

  STNNBR  CASTNO ROSETTE     LAT     LON  CTDPRS  CTDTMP  CTDSAL  CTDOXY  CTDCHL   THETA   SIGMA  OXYGEN     DIC ALKALIN  PHSPHT NO2+NO3  SILCAT     DOP     DON     DOC     TDP     TDN     LLN     LLP      PC      PN      PP     PSi  CHL A.   PHEO. CHLDA A    CHL+   PERID  19 BUT    FUCO  19 HEX PRASINO    VIOL DIADINO   ALLOX  LUTEIN  ZEAXAN   CHL B   A.CAR   B.CAR DV.CHLA MV.CHLA HPLCchl  H.BACT  P.BACT  S.BACT  E.BACT     ATP     CH4     N2O   QUALT1  QUALT2  QUALT3  QUALT4  QUALT5  QUALT6  QUALT7  QUALT8
                POSITION  DEG(N)  DEG(W)    DBAR   DEG C  PSS-78 UMOL/KG    UG/L  ITS-90   KG/M3 UMOL/KG UMOL/KG  UEQ/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG UMOL/KG NMOL/KG NMOL/KG    UG/L    UG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    NG/L    #/ML    #/ML    #/ML    #/ML   NG/KG NMOL/KG NMOL/KG                                                                *
                                                 ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* ******* *******                                                                *

       1       1      24 -16.001 170.007    13.4  29.086  35.207   197.8  0.1875  29.083 22.1942    -9.0    -9.0      -9   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00  -9.000  -9.000   -9.00   -9.00   -9.00  -9.000   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00   -9.00  -9.000  -9.000  -9.000  -9.000   -9.00   -9.00   -9.00  2222229 9999999  999999  955559  999999  999999  999995  555555

    The files are self-explanatory; one column is written for each
measured parameter.  Missing data are filled with -9. A five-line
heading labels each column.  Variables having asterisks in their
heading have a quality flag associated with them. These quality flags
are concatenated as a quality word which is listed as the last eight
variables in each row (either six or seven flags per variable).  The
values each digit can assume and their meanings follows:

Quality Indicators:

	Flag  Meaning
	 1    not quality controled
	 2    good data
	 3    suspect (i.e.  questionable) data
	 4    bad data
	 5    missing data
	 9    variable not measured during this cast




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


def process_bottle():
    bottle_path = CMORE_BULA_path + "bottle/bula1.gof"
    df = pd.read_csv(
        bottle_path,
        skiprows=5,
        index_col=False,
        delim_whitespace=True,
        names=[
            "station",
            "cast",
            "rosette",
            "lat",
            "lon",
            "CTD_pres",
            "CTD_temp",
            "CTD_sal",
            "CTD_doxy",
            "CTD_chl",
            "theta",
            "sigma",
            "bottle_oxygen",
            "DIC",
            "alkalinity",
            "phosphate",
            "NO2_NO3",
            "silcate",
            "DOP",
            "DON",
            "DOC",
            "TDP",
            "TDN",
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
            "qual_1",
            "qual_2",
            "qual_3",
            "qual_4",
            "qual_5",
            "qual_6",
            "qual_7",
            "qual_8",
        ],
    )
    df[
        [
            "CTD_temp_flag",
            "CTD_sal_flag",
            "CTD_doxy_flag",
            "CTD_chl_flag",
            "theta_flag",
            "sigma_flag",
            "bottle_oxygen_flag",
        ]
    ] = (
        df["qual_1"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "CTD_temp_flag",
            "CTD_sal_flag",
            "CTD_doxy_flag",
            "CTD_chl_flag",
            "theta_flag",
            "sigma_flag",
            "bottle_oxygen_flag",
        ]
    ] = (
        df["qual_1"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "DIC_flag",
            "alkalinity_flag",
            "phosphate_flag",
            "NO2_NO3_flag",
            "silcate_flag",
            "DOP_flag",
            "DON_flag",
        ]
    ] = (
        df["qual_2"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[["DOC_flag", "TDP_flag", "TDN_flag", "LLN_flag", "LLP_flag", "PC_flag"]] = (
        df["qual_3"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[["PN_flag", "PP_flag", "PSi_flag", "chl_a_flag", "pheo_flag", "chlda_flag"]] = (
        df["qual_4"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "chl_plus_flag",
            "PERID_flag",
            "19_but_flag",
            "FUCO_flag",
            "19_Hex_flag",
            "Prasino_flag",
        ]
    ] = (
        df["qual_5"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "Viol_flag",
            "Diadino_flag",
            "Allox_flag",
            "Lutein_flag",
            "Zeaxan_flag",
            "Chl_b_flag",
        ]
    ] = (
        df["qual_6"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "alpha_carotene_flag",
            "beta_carotene_flag",
            "divinyl_chl_flag",
            "monovinyl_chl_flag",
            "HPLC_chla_flag",
            "hetero_bact_flag",
        ]
    ] = (
        df["qual_7"].astype(str).str.extractall("(.)")[0].unstack()
    )
    df[
        [
            "prochloro_bact_flag",
            "synecho_bact_flag",
            "eukaryotes_flag",
            "ATP_flag",
            "CH4_flag",
            "N2O_flag",
        ]
    ] = (
        df["qual_8"].astype(str).str.extractall("(.)")[0].unstack()
    )
