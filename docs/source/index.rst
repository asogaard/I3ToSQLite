I3ToSQLite
========

$project will solve your problem of where to start with documentation,
by providing a basic explanation of how to do it easily.

A minimalistic example:
.. highlight:: python
   from I3ToSQLite.Tools import CreateDatabase

   paths             = ['/groups/hep/pcs557/i3_workspace/data/real_data/level7_v02.00/IC86.11']
   db_name           = 'my_database'
   outdir            = '~/my_outdir'
   workers           = 42
   cvmfs_setup_path  = '/cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/setup.sh'
   cvmfs_shell_path = '/cvmfs/icecube.opensciencegrid.org/users/Oscillation/software/oscNext_meta/releases/latest/build/env-shell.sh'
   gcd_rescue = '/groups/icecube/stuttard/data/oscNext/pass2/gcd/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withScaledNoise.i3.gz'
   pulse_keys  = ['SRTInIcePulses']

   CreateDatabase(paths, outdir, workers, cvmfs_setup_path, cvmfs_shell_path, db_name,pulse_keys, gcd_rescue, verbose = 1)

Features
--------

- Be awesome
- Make things faster

Installation
------------

Install $project by running:

    install project

Contribute
----------

- Issue Tracker: github.com/$project/$project/issues
- Source Code: github.com/$project/$project

Support
-------

If you are having issues, please let us know.
We have a mailing list located at: project@google-groups.com

License
-------

The project is licensed under the BSD license.