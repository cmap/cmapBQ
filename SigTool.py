import os, traceback
from datetime import datetime

class SigTool:

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
            os.mkdir(path)

        if create_subdir:
            timestamp = datetime.now().strftime('_%Y%m%d%H%M%S')
            out_name = ''.join([toolname, timestamp])
            out_path = os.path.join(path, out_name)
            os.mkdir(out_path)
            return out_path
        else:
            return path

    def run(self):
        raise NotImplementedError
