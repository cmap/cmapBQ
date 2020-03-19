import SigTool
from google.auth import exceptions
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import os, argparse, sys, traceback


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')


class queryBQ:
    def __call__(self, *args, **kwargs):
        try:
            pass
            self.write_status(True, out_path)
        except exceptions.DefaultCredentialsError as cred_error:
            print('Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or'
                  ' specify path to key using --key')
            self.write_status(False, out_path, exception=cred_error)
            exit(1)
        except Exception as e:
            self.write_status(False, out_path, exception=e)
            exit(1)

    def parse_args(argv):
        parser = argparse.ArgumentParser()
        parser.add_argument('-a', '--from_table', help="Source Table, rows that exist in both will this table's value")
        parser.add_argument('-b', '--to_table', help="Destination Table, matching rows will have value overwritten")
        parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
        parser.add_argument("--in_place", type=str2bool, nargs='?',
                            const=True, default=False,
                            help="If false, to_table will be copied before performing merge. Default is False")
        parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
        parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
        args = parser.parse_args(argv)
        return args

    def write_args(args, out_path):
        options = vars(args)
        with open(os.path.join(out_path, 'config.txt'), 'w+') as f:
            for option in options:
                f.write("{}: {}\n".format(option, options[option]))
                print("{}: {}".format(option, options[option]))







if __name__ == '__main__':
    pass



