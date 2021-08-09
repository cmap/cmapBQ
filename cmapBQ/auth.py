import os


def _check_path_exists():
    PATH = os.path.expanduser('~/.cmapBQ')
    if os.path.exists(PATH):
        pass
    else:
        os.mkdir(PATH)
    return PATH

def _get_credentials():
    _check_path_exists()

