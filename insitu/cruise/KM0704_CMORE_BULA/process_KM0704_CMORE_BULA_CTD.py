import pandas as pd
import glob
import os
import re

from pandas.io.parsers import read_csv
from cmapingest import vault_structure as vs


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
        df[
            [
                "CTDPRS_flag",
                "CTDTMP_flag",
                "CTDSAL_flag",
                "CTDOXY_flag",
                "PAR_flag",
                "LS6000_flag",
                "CHLPIG_flag",
                "NITRATE_flag",
            ]
        ] = (
            df["QUALT1"].astype(str).str.extractall("(.)")[0].unstack()
        )
        df.drop(["QUALT1"], axis=1, inplace=True)

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

    df.drop(
        [
            "qual_1",
            "qual_2",
            "qual_3",
            "qual_4",
            "qual_5",
            "qual_6",
            "qual_7",
            "qual_8",
        ],
        axis=1,
        inplace=True,
    )
    return df


def process_wind():
    wind_concat_list = []
    wind_fpath = CMORE_BULA_path + "wind/"
    wind_flist = glob.glob(wind_fpath + "*.gz*")
    names = [
        "year",
        "jday",
        "hr",
        "min",
        "sec",
        "msec",
        "code",
        "port_wind_relative_speed",
        "port_wind_relative_heading",
        "SOG",
        "COG",
        "POSMV_HDG",
        "wind_true_speed",
        "wind_true_heading",
        "starboard_wind_relative_speed",
        "starboard_wind_relative_heading",
    ]
    for windf in wind_flist:
        df = pd.read_csv(windf, names=names, delim_whitespace=True)
        df["time"] = pd.to_datetime(
            (df["year"]).astype(str)
            + (df["jday"].astype(str))
            + (df["hr"].astype(str))
            + (df["min"].astype(str))
            + (df["sec"].astype(str).str.zfill(2))
            + (df["msec"].astype(str)),
            format="%Y%j%H%M%S%f",
        )
        df.drop(["year", "jday", "hr", "min", "sec", "msec"], axis=1, inplace=True)
        df = df[
            [
                "time",
                "port_wind_relative_speed",
                "port_wind_relative_heading",
                "SOG",
                "COG",
                "POSMV_HDG",
                "wind_true_speed",
                "wind_true_heading",
                "starboard_wind_relative_speed",
                "starboard_wind_relative_heading",
            ]
        ]
        wind_concat_list.append(df)

    concat_df = pd.concat(wind_concat_list, axis=0, ignore_index=True)
    return concat_df


# CTD
# Underway
# Underway Sample
# Bottle
# Wind


"""CTD NEEDS FLAG PROCESSING"""
