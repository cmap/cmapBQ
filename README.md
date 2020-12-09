# cmapBQ
**Author**: Anup Jonchhe (anup@broadinstitute.org)

`cmapBQ` allows for targeted retrieval of relevant gene expression data
from the resources provided by The Broad Institute and LINCS Project hosted on Google BigQuery.

## Installation
cmapBQ is available to install through `pip` with the command:

`pip install cmapBQ` or `pip install --upgrade cmapBQ`

to get the latest version.

### Credentials Setup


A Google Cloud Account is necessary to run jobs on BigQuery to retrieve data. 
A setup guide for obtaining the required JSON credentials key can be found below:

https://cmapbq.readthedocs.io/en/latest/setup-guide.html

### Where to place your JSON service file
   
The service account credentials should be placed in the `~/.cmapBQ` folder 
and the `~/.cmapBQ/config.txt` file should be edited such that the
credentials option points to the path of the service account credentials:
    
    credentials: /path/to/service-credentials.json

Alternatively, the following command can be run from within a python session after 
installation. This only needs to be run once

    import cmapBQ.query as cmap_query
    import cmapBQ.config as cmap_config
    
    cmap_config.setup_credentials(path_to_json)

##Tutorials and Documentation
A demo of `cmapBQ` functionality is available at this [Github repo](https://github.com/cmap/lincs-workshop-2020) 
and accessible through Colab: 

[cmapBQ Demo Notebook](https://colab.research.google.com/github/cmap/lincs-workshop-2020/blob/main/BQ_toolkit_demo.ipynb)

Documentation is available on ReadTheDocs here: https://cmapbq.readthedocs.io
