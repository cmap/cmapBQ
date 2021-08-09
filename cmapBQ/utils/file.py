import os

import pandas as pd

from cmapPy.pandasGEXpress.GCToo import GCToo
from cmapPy.pandasGEXpress.write_gctx import write as write_gctx
from cmapPy.pandasGEXpress.write_gct import write as write_gct


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
