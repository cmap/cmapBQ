import re
import sys, argparse
import pandas as pd
import time, os, traceback
from datetime import datetime
from cmapPy.pandasGEXpress.GCToo import GCToo
from cmapPy.pandasGEXpress import write_gct, write_gctx

### Argument management: To be moved to a class in the future
def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def parse_args(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('csv_file', help="input CSV file to convert to  GCT(x) file, "
                                         "must contain rid, cid, value columns")
    parser.add_argument('-f', '--outfile', help="Output file of GCT(x) file", default="")
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, nargs='?',
                            const=True, default=True)
    parser.add_argument('-g', '--use_gctx', help="Use GCTX file format", type=str2bool, nargs='?',
                            const=True, default=False)
    if argv is None:
        parser.print_help()
        sys.exit(1)
    else:
        args = parser.parse_args(argv)
        return args

### Main functionality
def pivot_CSV(csv_path, args):
    data = pd.read_csv(csv_path)
    data = data.loc[:, ['rid', 'cid', 'value']]
    gct_df = data.pivot(index='rid', columns='cid', values='value')
    return gct_df

### Output code: To be moved to a class in the future
def write_args(args, out_path):
    options = vars(args)
    with open(os.path.join(out_path, 'config.txt'), 'w+') as f:
        for option in options:
            f.write("{}: {}\n".format(option, options[option]))
            print("{}: {}".format(option, options[option]))

def write_status(success, out, exception=""):
    if success:
        print("Successfully writted output to {}".format(out))
        with open(os.path.join(out, 'SUCCESS.txt'), 'w') as file:
            file.write("Finished on {}\n".format(datetime.now().strftime('%c')))
    else:
        print(str(exception))
        print(traceback.format_exc())
        print("Output and stack traced saved to {}".format(out))
        with open(os.path.join(out, 'FAILURE.txt'), 'w') as file:
            file.write(str(exception))
            file.write(traceback.format_exc())

def mk_out_dir(path, toolname, create_subdir=True):
    path = os.path.abspath(path)
    if not os.path.exists(path):
        os.makedirs(path)

    if create_subdir:
        timestamp = datetime.now().strftime('_%Y%m%d%H%M%S')
        out_name = ''.join([toolname, timestamp])
        out_path = os.path.join(path, out_name)
        os.makedirs(out_path)
        return out_path
    else:
        return path


def main(argv):
    args = parse_args(sys.argv[1:])
    csv_path = args.csv_file

    out_path = mk_out_dir(args.out, "csv2gct", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.outfile == "":
        if args.use_gctx:
            out_file = re.sub(".csv", ".gctx", csv_path)
        else:
            out_file = re.sub(".csv", ".gct", csv_path)
    else:
        out_file = args.outfile

    file_name = os.path.basename(out_file)
    out_file = os.path.join(out_path, file_name)
    try:
        data_df = pivot_CSV(csv_path, args)

        GCT_obj = GCToo(data_df=data_df)

        if args.use_gctx:
            write_gctx.write(GCT_obj, out_file)
        else:
            write_gct.write(GCT_obj, out_file)

        write_status(True, out_path)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)

if __name__ == "__main__":
    start_time = time.time()
    main(sys.argv[1:])
    print ("--- Total Time: {0} seconds ---".format(time.time() - start_time))
