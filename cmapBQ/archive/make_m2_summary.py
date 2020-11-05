import pandas as pd
import os as os
from cmapPy.set_io import grp


def summ_function(flist, proj_dir="/cmap/obelix/pod/custom/"):
    list = grp.read(flist)
    proj_name = {"name": list}
    summary = pd.DataFrame(proj_name, index=list)
    build_paths = []
    found_tf = []
    for proj in list:
        if proj.startswith("/"):  # is list item a full path
            proj_build = proj
        else:
            proj_build = os.path.join(proj_dir, proj, "builds", ".*modzs.*")

        # Check if path exists
        if os.path.exists(proj_build):
            found_tf.append(1)
            build_paths.append(proj_build)
        else:
            found_tf.append(0)
            build_paths.append(proj_build)
        print(proj_build)

    # append to dataframe
    summary["build_path"] = build_paths
    summary["found"] = found_tf
    return summary


if __name__ == "__main__":
    table = summ_function("/cmap/projects/M2/build/projects_to_release.grp")
    table.to_csv("files/summary.txt", sep="\t", index=False)
