import os
import argparse
import traceback
from datetime import datetime

import pandas as pd

from cmapPy.pandasGEXpress.GCToo import GCToo
from cmapPy.set_io.grp import read as parse_grp
from cmapPy.pandasGEXpress.write_gctx import write as write_gctx
from cmapPy.pandasGEXpress.write_gct import write as write_gct


def parse_condition(arg, sep=","):
    """
    Parse argument for pathname, string or list. If file path exists reads GRP or TXT file.
    Non-path filenames are tokenized by specified delimiter, default is ','.
    Returns list

    :param arg: Takes in pathname, string, or list.
    :param sep: Delimiter to separate elements in string into list. Default is ','
    :return: list
    """
    if isinstance(arg, str):
        if os.path.isfile(arg):
            arg = parse_grp(arg)
        else:
            arg = arg.split(sep=sep)
    return list(arg)

def str2bool(v):
    if isinstance(v, bool):
        return v
    if v.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif v.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError("Boolean value expected.")


def write_args(args, out_path):
    options = vars(args)
    with open(os.path.join(out_path, "config.txt"), "w+") as f:
        for option in options:
            f.write("{}: {}\n".format(option, options[option]))
            print("{}: {}".format(option, options[option]))


def write_status(success, out, exception=""):
    if success:
        print("SUCCESS: Output written to {}".format(out))
        with open(os.path.join(out, "SUCCESS.txt"), "w") as file:
            file.write("Finished on {}\n".format(datetime.now().strftime("%c")))
    else:
        print("FAILED: Stack traced saved to {}".format(out))
        with open(os.path.join(out, "FAILURE.txt"), "w") as file:
            file.write(str(exception))
            file.write(traceback.format_exc())


def mk_out_dir(path, toolname, create_subdir=True):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.mkdir(path)

    if create_subdir:
        timestamp = datetime.now().strftime("_%Y%m%d%H%M%S")
        out_name = "".join([toolname, timestamp])
        out_path = os.path.join(path, out_name)
        os.mkdir(out_path)
        return out_path
    else:
        return path


def long_to_gctx(df):
    """
        Converts long csv table to GCToo Object. Dataframe must have 'rid', 'cid' and 'value' columns
        No other columns or metadata is preserved.

    :param df: Long form pandas DataFrame
    :return: GCToo object
    """
    df = df[["rid", "cid", "value"]].pivot(index="rid", columns="cid", values="value")
    gct = GCToo(df)

    # Ensure index is string
    gct.row_metadata_df.index = gct.row_metadata_df.index.astype("str")
    gct.data_df.index = gct.data_df.index.astype("str")

    return gct

def csv_to_gctx(filepaths, outpath, use_gctx=True):
    """
        Convert list of csv files to gctx. CSVs must have 'rid', 'cid' and 'value' columns
        No other columns or metadata is preserved.

    :param filepaths: List of paths to CSVs
    :param outpath: output directory of file
    :param use_gctx: use GCTX HDF5 format. Default is True
    :return:
    """
    li = []
    for filename in filepaths:
        df = pd.read_csv(filename, index_col=None, header=0)
        li.append(df)
    result = pd.concat(li, axis=0, ignore_index=True)
    df = result[["rid", "cid", "value"]].pivot(
        index="rid", columns="cid", values="value"
    )
    gct = GCToo(df)
    if use_gctx:
        ofile = os.path.join(outpath, "result.gctx")
        write_gctx(gct, ofile)
    else:
        ofile = os.path.join(outpath, "result.gct")
        write_gct(gct, ofile)

    return ofile
