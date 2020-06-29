import re
import sys, argparse
import pandas as pd
import time, os, traceback
from datetime import datetime

from cmapPy.pandasGEXpress.parse import parse
from cmapBQ.utils import str2bool

def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help="input GCTX file to convert to longform parquet")
    parser.add_argument('-f', '--outfile', help="Output file of Parquet file", default="")
    parser.add_argument('-k', '--chunk_size', help="Chunk size to read and pivot columns",default=1000, type=int)
    parser.add_argument('-w', '--staged_write',
                        help="Number of columns at which to save Parquet files",
                        default=10**5, type=int) #10**5 equated to about a 5GB file
    parser.add_argument('-s','--start', help="Starting index. Useful for parallelization", default=0, type=int)
    parser.add_argument('-e','--end', help="End index. Useful for parallelization", default=-1, type=int)
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    args = parser.parse_args(argv)
    return args


def parse_and_pivot(dspath, start, end):
    ds = parse(dspath, cidx=range(start, end))
    ds.data_df["rid"] = ds.data_df.index

    #    print('Melting dataset into long form')
    ch_long = ds.data_df.melt(id_vars=["rid"])

    ch_long["rid"] = ch_long["rid"].astype(str)
    ch_long["cid"] = ch_long["cid"].astype(str)
    return ch_long


def convert_and_write(dspath, outfile, args):
    chunk_size=args.chunk_size
    staged_write=args.staged_write
    start, end = args.start, args.end
    print ("Reading dataset: " + dspath)
    ds_meta = parse(dspath, col_meta_only=True)
    if end == -1:
        c = len(ds_meta)
        print ("Dataset size:", c)
    else:
        c = end
        print ("Dataset size:", end-start)

    ds_long = pd.DataFrame()
    curr, ncol, stage = start, 0, 1
    while curr != c:
        if curr + chunk_size < c:
            print ("Converting chunk of size {0}: {1}/{2}".format(chunk_size, curr, c))
            ch_long = parse_and_pivot(dspath, curr, curr + chunk_size)
            if ds_long.empty:
                ds_long = ch_long
            else:
                ds_long = ds_long.append(ch_long)
            curr += chunk_size
            ncol += chunk_size
        else:
            # Finish
            print ("Converting chunk of size {0}: {1}/{2}".format(c - curr, curr, c))
            ch_long = parse_and_pivot(dspath, curr, c)
            ds_long = ds_long.append(ch_long)
            curr = c
        # Write to file
        if staged_write == 0:
            print ("done.")
            pass
        elif ncol >= staged_write:
            ncol = 0
            stage_out = re.sub("\.parquet$", "_{0}.parquet".format(stage), outfile)
            stage += 1
            print ("Saving parquet to {file}".format(file=stage_out))
            ds_long.to_parquet(stage_out)
            ds_long = pd.DataFrame()

        if curr == c:
            if stage > 1:
                stage_out = re.sub("\.parquet$", "_{0}.parquet".format(stage), outfile)
                stage += 1
                print ("Saving parquet to {file}".format(file=stage_out))
                ds_long.to_parquet(stage_out)
            else:
                ds_long.to_parquet(outfile)

## To be moved to a class in the future

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
    args = parse_args(argv)
    dspath = args.infile

    out_path = mk_out_dir(args.out, "gctx2parquet", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.outfile == "":
        out_file = re.sub(".gctx", ".parquet", dspath)
    else:
        out_file = args.outfile

    out_file = os.path.join(out_path, out_file)
    try:
        convert_and_write(dspath, out_file, args)
        write_status(True, out_path)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)



if __name__ == "__main__":
    start_time = time.time()
    print(sys.argv)
    main(sys.argv[1:])
    print ("--- Total Time: {0} seconds ---".format(time.time() - start_time))
