import pandas as pd
import os
from cmapPy.pandasGEXpress.parse import parse


def find_failure(ds_long):
    if len(ds_long) < 100:
        return ds_long
    ds_len = len(ds_long)
    try:
        print("Writing to parquet length: " + str(ds_len))
        ds_long[0 : ds_len / 2].to_parquet("out.parquet", index=False)
        print("Attempting to read...")
        ds = pd.read_parquet("out.parquet")
        print("success")
        return find_failure(ds_long[ds_len / 2 :].copy())
    except:
        print("failed")
        return find_failure(ds_long[0 : ds_len / 2].copy())


if __name__ == "__main__":
    dspath = "/cmap/projects/M2/build/modzs_n1460792x12328.gctx"
    print("Running")
    stage = 10 ** 4
    tempdir = "files"

    print("Reading " + dspath)
    df = parse(dspath, cidx=range(290000, 300000))
    print("Done.")

    df.data_df["rid"] = df.data_df.index
    print("Converting into long form")
    ds_long = df.data_df.melt(id_vars="rid")
    print("Done.")

    problem_set = find_failure(ds_long)
    print(problem_set)
