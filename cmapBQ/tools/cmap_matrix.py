import os, sys
import argparse

import pandas as pd
from google.cloud import bigquery
from google.auth import exceptions

from cmapBQ.utils import write_args, write_status, mk_out_dir, str2bool
from cmapBQ.query import cmap_matrix
from cmapPy.pandasGEXpress.write_gctx import write as write_gctx
from cmapPy.pandasGEXpress.write_gct import write as write_gct

toolname = "cmap_matrix"
description = "Download table hosted on BiqQuery as a GCTX"


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="cmapBQ {}".format(toolname), description=description
    )
    parser.add_argument("--table", help="Table to query", default=None)
    parser.add_argument("--cid", help="List of sig_ids to extract", default=None)
    parser.add_argument("--rid", help="List of moas to query", default=None)
    parser.add_argument(
        "--chunk_size",
        help="Size of each chunk as a number of columns from --cid",
        default=10000,
        type=int,
    )

    tool_group = parser.add_argument_group("Tool options")
    tool_group.add_argument(
        "-f", "--filename", help="Name of output file", default="result.gctx"
    )
    tool_group.add_argument(
        "-k",
        "--key",
        help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS",
        default=None,
    )
    tool_group.add_argument("-o", "--out", help="Output folder", default=os.getcwd())
    tool_group.add_argument(
        "-c", "--create_subdir", help="Create Subdirectory", type=str2bool, default=True
    )
    tool_group.add_argument(
        "-g",
        "--use_gctx",
        help="Use GCTX format, default is true",
        type=str2bool,
        default=True,
    )
    tool_group.add_argument(
        "-v", "--verbose", help="Run in verbose mode", type=str2bool, default=False
    )

    if argv:
        args = parser.parse_args(argv)
        return args
    else:
        parser.print_help()
        sys.exit(1)


def main(argv):
    args = parse_args(argv)
    out_path = mk_out_dir(args.out, toolname, create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    try:
        bq_client = bigquery.Client()

        gct = cmap_matrix(
            bq_client,
            table=args.table,
            rid=args.rid,
            cid=args.cid,
            verbose=args.verbose,
            chunk_size=args.chunk_size,
        )

        fn = os.path.splitext(os.path.basename(args.filename))[0]
        shape = gct.data_df.shape

        if args.use_gctx:
            fn = "{}_n{}x{}.gctx".format(fn, shape[1], shape[0])
            ofile = os.path.join(out_path, fn)
            write_gctx(gct, ofile)
        else:
            fn = "{}_n{}x{}.gct".format(fn, shape[1], shape[0])
            ofile = os.path.join(out_path, fn)
            write_gct(gct, ofile)

        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print(
            "Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or"
            " specify path to key using --key"
        )
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)


if __name__ == "__main__":
    main(sys.argv[1:])
