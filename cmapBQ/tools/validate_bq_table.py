from google.cloud import bigquery
from google.auth import exceptions
from cmapPy.pandasGEXpress.parse import parse
from cmapPy.set_io import grp
from datetime import datetime
import numpy as np
import pandas as pd
import os, argparse, sys, random, traceback


def str2bool(v):
    if isinstance(v, bool):
       return v
    if v.lower() in ('yes', 'true', 't', 'y', '1'):
        return True
    elif v.lower() in ('no', 'false', 'f', 'n', '0'):
        return False
    else:
        raise argparse.ArgumentTypeError('Boolean value expected.')

def write_args(args, out_path):
    options = vars(args)
    with open(os.path.join(out_path, 'config.txt'), 'w+') as f:
        for option in options:
            f.write("{}: {}\n".format(option, options[option]))
            print("{}: {}".format(option, options[option]))


def parse_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('gct_source', help="Input Table")
    parser.add_argument('table', help="Reference Table")
    parser.add_argument('-s', '--sig_ids', help="GRP file with sig_ids to use in sampling", default="")
    parser.add_argument('-n', '--samples', help="Number of columns to sample. Default is 100", default=100, type=int)
    parser.add_argument('-g', '--genes', help="Number of columns to sample. Default is all", default=None, type=int)
    parser.add_argument('-k', '--key', help="Path to service account key. \n Alternatively, set GOOGLE_APPLICATION_CREDENTIALS", default=None)
    parser.add_argument('-o', '--out', help="Output folder", default=os.getcwd())
    parser.add_argument('-c', '--create_subdir', help="Create Subdirectory", type=str2bool, default=True)
    args = parser.parse_args(argv)
    return args

def rowIter_to_df(rowIter):
    dict = []
    for row in rowIter:
        dict.append({'rid':row.rid, 'cid':row.cid, 'value': row.value})
    df = pd.DataFrame(data=dict)
    df = df.pivot(index='rid', columns='cid', values='value')
    return df

def run_validation(gct_file, table, args):
    nsampl = args.samples
    ngenes = args.genes

    try:
        client = bigquery.Client()
    except exceptions.DefaultCredentialsError:
        raise

    col_meta = parse(gct_file, col_meta_only=True)
    row_meta = parse(gct_file, row_meta_only=True)

    nsampl = min(len(col_meta), nsampl)
    if args.sig_ids == "" or not os.path.exists(args.sig_ids):
        print ("Randomly sampling {} sig ids".format(nsampl))
        col_idx = random.sample(range(0,len(col_meta)), nsampl)
        sample_ids = list(col_meta.iloc[col_idx].index)
    else:
        print ("Reading {} for sig_ids".format(args.sig_ids))
        sample_ids = list(grp.read(args.sig_ids))
        print ("{} sig_ids found.".format(len(sample_ids)))

    if ngenes is not None:
        row_idx = random.sample(range(0,len(row_meta)), ngenes)
        sample_rows = list(row_meta.iloc[row_idx].index)
    else:
        #row_idx = random.sample(range(0,len(row_meta)), ngenes)
        sample_rows = list(row_meta.iloc[:].index)

    #print(sample_ids)
    print ("Slicing GCT file")
    gct_sample = parse(gct_file, cid=sample_ids, rid=sample_rows) #takes the most time
    print ("Done.")

    query = ("SELECT * FROM `{}` WHERE cid in UNNEST({}) AND rid in  UNNEST({})".format(table, sample_ids, sample_rows))

    #print(query)
    print ("Running Query")
    query_job = client.query(query)
    bq_res = query_job.result()
    print ("Done.")

    bq_mat = rowIter_to_df(bq_res)
    gct_mat = gct_sample.data_df

    #assert (gct_mat.shape == bq_mat.shape, "Matrix shapes are dissimilar. Genes or Signature ids may be missing")  #shape check

    gct_mat.sort_index()    #sort to match genes
    bq_mat.sort_index()

    rpt = []
    errors = []
    for sig_id in gct_mat.columns:
        try:
            tf = abs(gct_mat[sig_id] - bq_mat[sig_id]) < 0.001
            rpt.append({'sig_id': sig_id, 'num_valid': np.count_nonzero(tf), 'num_invalid': np.count_nonzero(~tf)})
            if np.count_nonzero(~tf) > 0:
                        errors.append({'sig_id': sig_id, 'error':'incorrect values'})
        except KeyError:
            print('WARNING: Key not found: {}'.format(sig_id))
            errors.append({'sig_id':sig_id, 'error': 'KeyError'})

    rpt = pd.DataFrame(data=rpt, columns=['sig_id', 'num_valid', 'num_invalid'])
    errors  = pd.DataFrame(data=errors, columns=['sig_id', 'error'])

    return rpt, errors

def write_status(success, out, exception=""):
    if success:
        print("Successfully writted output to {}".format(out))
        with open(os.path.join(out, 'SUCCESS.txt'), 'w') as file:
            file.write("Finished on {}\n".format(datetime.now().strftime('%c')))
    else:
        print(traceback.format_exc())
        print("FAILURE: Output and stack traced saved to {}".format(out))
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

def main(args=None):
    args = parse_args(args)
    out_path = mk_out_dir(args.out, "validate_BQ_table", create_subdir=args.create_subdir)
    write_args(args, out_path)

    if args.key is not None:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.key

    try:
        rpt, errors = run_validation(args.gct_source, args.table, args)
        rpt.to_csv(os.path.join(out_path, 'report.txt'), sep='\t', index=False)
        errors.to_csv(os.path.join(out_path, 'errors.txt'), sep='\t', index=False)
        write_status(True, out_path)
    except exceptions.DefaultCredentialsError as cred_error:
        print('Could not automatically determine credentials. Please set GOOGLE_APPLICATION_CREDENTIALS or'
              ' specify path to key using --key')
        write_status(False, out_path, exception=cred_error)
        exit(1)
    except Exception as e:
        write_status(False, out_path, exception=e)
        exit(1)



if __name__ == '__main__':
    main(sys.argv[1:])
