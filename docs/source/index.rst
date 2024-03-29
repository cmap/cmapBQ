.. cmapBQ documentation master file, created by
   sphinx-quickstart on Thu Nov 12 16:14:22 2020.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

cmapBQ's Documentation
==================================

Installation Guide
^^^^^^^^^^^^^^^^^^

The ``cmapBQ`` toolkit is available on PyPi and can be installed using:
    ``pip install cmapBQ``

This will install the package into your currently active environment.

From here, ensure you have a Google Service Account Credentials file, documentation can be found at `Getting started with authentication
<https://cloud.google.com/docs/authentication/getting-started>`_


It is recommended to place the credentials in the ~/.cmapBQ folder. To complete installation, run the following command from within a python session. This only needs to be done once, and will populate a ~/.cmapBQ/config.txt file with default table values and correct credentials path.

 
.. code-block:: python
   :emphasize-lines: 3,5

   import cmapBQ.config as cmap_config
   cmap_config.setup_credentials('~/.cmapBQ/credentials-file.json') #or path to credentials file


Guide
^^^^^
* :ref:`search`
.. toctree::
   :maxdepth: 3

   cmapBQ
   help
   license
   :caption: Contents:



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`

