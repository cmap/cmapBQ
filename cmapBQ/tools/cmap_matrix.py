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
    parser = argparse.ArgumentParser(prog="cmapBQ {}".format(toolname), description=description)
    parser.add_argument('--table', help="Table to query", default=None)
    parser.add_argument('--cids', help="List of sig_ids to extract", default=None)
    parser.add_argument('--rids', help="List of moas to query", default=None)


    tool_group = parser.add_argument_group('Tool options')
    tool_group.add_argument('-f', '--filename', help="Name of output file", default="result.gctx")
    tool_group.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    tool_group.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    tool_group.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    tool_group.add_argument('-g', '--use_gctx', help="Use GCTX format, default is true", default=True)


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

        gct = cmap_matrix(bq_client, table=args.table, rids=args.rids, cids=args.cids)

        if args.use_gctx:
            ofile = os.path.join(out_path,'result.gctx')
            write_gctx(gct, ofile)
        else:
            ofile = os.path.join(out_path,'result.gct')
            write_gct(gct, ofile)

        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print('Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or'
              ' specify path to key using --key')
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)
