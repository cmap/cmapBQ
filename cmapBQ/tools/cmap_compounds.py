import os, sys
import argparse

import pandas as pd
from google.cloud import bigquery
from google.auth import exceptions

from cmapBQ.utils import write_args, write_status, mk_out_dir, str2bool
from cmapBQ.query import cmap_compounds

def parse_args(argv):
    parser = argparse.ArgumentParser(
        description="Query Compound Info table for MoA, Target, BRD information"
    )
    parser.add_argument("--pert_id", help="List of pert_id to query", default=None)
    parser.add_argument("--cmap_name", help="List of cmap_names to query", default=None)
    parser.add_argument("--moa", help="List of moas to query", default=None)
    parser.add_argument("--target", help="List of targets to query", default=None)
    parser.add_argument(
        "--compound_aliases", help="List of compound aliases to query", default=None
    )

    tool_group = parser.add_argument_group("Tool options")
    tool_group.add_argument(
        "-f", "--filename", help="Name of output file", default="result.txt"
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

    if argv:
        args = parser.parse_args(argv)
        return args
    else:
        parser.print_help()
        sys.exit(1)


def main(argv):
    args = parse_args(argv)
    out_path = mk_out_dir(args.out, "cmap_compounds", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    try:
        bq_client = bigquery.Client()

        result = cmap_compounds(
            bq_client,
            pert_id=args.pert_id,
            cmap_name=args.cmap_name,
            moa=args.moa,
            target=args.target,
            compound_aliases=args.compound_aliases,
        )

        result.to_csv(os.path.join(out_path, args.filename), sep="\t", index=False)
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
