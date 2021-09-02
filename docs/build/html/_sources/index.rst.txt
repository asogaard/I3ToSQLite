I3ToSQLite
========

I3ToSQLite is an unofficial IceCube i3-file converter that exports the desired contents of the i3 files to SQLite format.

Features
--------
- Multiprocessed i3-file reading
- Memory efficient database creation
- Simple usage
- Supports multiple pulse maps
- Allows for easy back-tracking to original PFrame if needed
 
Why SQLite?
--------
SQlite is a robust, well understood software library that a quite common choice for data storage in the data science community. Specifically for reconstruction purposes, the format is interesting because:

 - Because the database is indexed, you only load the data you query into memory.
 - Query times happens in log(n), where n is the amount of rows in the database table you're querying, making query execution times fast enough for the database to be used direcly in your preprocessing, or as a dataset in your reconstruction.
 - Once the database is created, users do not need to understand anything related to IceTray, which in turn means newcommers have more time to work on their analysis. 
 - The low memory footprint of the database makes it portable to personal devices.
 - A SQLite database can recieve multiple queries at the same time, which makes it an ideal format for a standardized dataset that is centrally stored.

Installation
========

Install I3ToSQLite by running:

.. code-block:: sh
   :linenos:
   
   pip install I3ToSQLite

CreateDatabase():
========
.. py:function:: CreateDatabase(paths : list, outdir: str, workers: int, cvmfs_setup_path: str, cvmfs_shell_path: str, db_name: str, pulse_keys: list, gcd_rescue: str, verbose = 1)

\
   Reads the i3-files in *paths* through the CVMFS version and shell specified in *cvmfs_setup_path* and *cvmfs_shell_path*. The i3 files are converted into a SQLite database stored at *outdir*/*db_name*/*db_name*.db. 

============
Arguments
============
- paths : list | a list of paths to directories containing the i3 files you wish to convert. 
          

                        *example*: ['~/genie/level7_v02.00','~/muongun/level7_v02.00']

               **Please Note**: Currently, only the specified directories and two-level sub directories are scanned. E.g. specifying paths = [*genie/level7_v02.00*] means extracting everything in  *genie/level7_v02.00/runid/subrunid* as per oscNext IceCube convention. If your files are located beyond the second level subdirectories, you'll have to adjust your file structure.

- outdir  : string | a string specifying the directory to which the database will be written. 

- workers : integer | the number of cores used to read i3 files. Must be positive and non-zero.
        Note that *workers* will default to the number of i3 files in the specified paths if the amount of allocated cores are larger than the amount of i3 files in the selected paths.

- cvmfs_setup_path : string | path to your cvmfs setup. 
                    *example*: cvmfs_setup_path = '/cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/setup.sh'   

- cvmfs_shell_path : string | path to your cvmfs shell. 
                    *example*: cvmfs_shell_path = '/cvmfs/icecube.opensciencegrid.org/users/Oscillation/software/oscNext_meta/releases/latest/build/env-shell.sh'

- db_name : string | the name of your database. Please omit '.db' or other extensions.


- pulse_keys : list |  a list of i3 keys specifying the pulse maps that you want to extract. 
                     *example* : *pulse_keys  = ['SRTInIcePulses', 'SplitInIcePulses']*
                 **Please Note**: Extracting the pulse maps is the most expensive task in the extraction. Therefore, if you wish to speed up the process, limit yourself to only the pulse maps you need. Also; the extraction simply skips any pulse maps not present in the PFrames. It wont throw an error!

- gcd_rescue : string | a path to a gcd file. 
               **Please Note**: Usually both real data and mc data have GCD-files in the subrun folders. Therefore, the extraction checks every subfolder for the presence of a GCD file and uses it to extract the pulse information, but should there not be one, the extraction will default to the *gcd_rescue*.

- verbose : integer | the verbosity of the multiprocessed i3 file extraction. verbose = 0 means no prints, verbose > 0 currently gives full information. 

A minimalistic database creation example:
========
.. code-block:: python
   :linenos:

   from I3ToSQLite.Tools import CreateDatabase

   paths             = ['~/level7_v02.00/IC86.11']
   db_name           = 'my_database'
   outdir            = '~/my_outdir'
   workers           = 42
   cvmfs_setup_path  = 'some_path/setup.sh'
   cvmfs_shell_path = 'some_path/env-shell.sh'
   gcd_rescue = '~/some_GCD.i3.gz'
   pulse_keys  = ['SRTInIcePulses']

   CreateDatabase(paths, outdir, workers, cvmfs_setup_path, cvmfs_shell_path, db_name,pulse_keys, gcd_rescue, verbose = 1)

Which creates a database named *my_database* in 

.. code-block:: sh
   :linenos:

   ~/my_outdir/my_database/my_database.db

and a configuration file for later reference in 

.. code-block:: sh
   :linenos:

   ~/my_outdir/my_database/config/config.pkl

The Event Number:
========
The SQLite databases produced are indexed using a variable called *event_no*. The event number is an integer value that is unique within the database only, and has **nothing** to do with the event header information in the i3 files. This event number is used to match pulse information, reconstructions and truth information associated with each event. It is therefore the principal key used for querying the databases.´


**Please Note**: Because *event_no* is unique only within a database, you cannot use it to cross reference between different databases.


Example usage of a database:
========
You can check the available tables in a database by 

.. code-block:: python
   :linenos:

   import sqlite3
   db = 'path_to_database'

   with sqlite3.connect(db) as con:
      cursorObj = con.cursor()
      print('Available Tables: ')
      cursorObj.execute("SELECT name FROM sqlite_master WHERE type='table';")
      print(cursorObj.fetchall())

this will print the names of the available tables in the database. The database should contain *truth*, *RetroReco* if available, and at least one pulse map table, which carries the same name as the pulse map key, E.g. 'SRTInIcePulses'.


To extract event data we should start by extracting the available event numbers, such that we can use these to match between tables. To get the available event numbers, simply

.. code-block:: python
   :linenos:

   import sqlite3

   def extract_all_event_numbers(db):
      with sqlite3.connect(db) as con:
         query = 'from truth select event_no'
         event_numbers = pd.read_sql(query, con)
      return event_numbers

   db = 'path_to_database'
   event_numbers = extract_all_event_numbers(db)
   

*event_numbers* is now a pandas.DataFrame containing the full list of unique event numbers in the database. 

Suppose we then wanted to extract the pulse information in *SRTInIcePulses* associated with the first event, we can simply 

.. code-block:: python
   :linenos:

   import sqlite3

   def extract_all_event_numbers(db):
      with sqlite3.connect(db) as con:
         query = 'from truth select event_no'
         event_numbers = pd.read_sql(query, con)
      return event_numbers

   db = 'path_to_database'
   event_numbers = extract_all_event_numbers(db)

   our_event_of_interest = event_numbers['event_no][0]
   with sqlite3.connect(db) as con:
      query = 'from SRTInIcePulses select * where event_no == '%our_event_of_interest
      pulses = pd.read_sql(query, con)

*pulses* now contain the all the extracted information associated with the specific event. If you wanted to extract all the information available in the database from the RetroReco table, all you'd have to do is to change 'SRTInIcePulses' with 'RetroReco'.

Suppose you wanted to extract the pulse information for a batch of events, let's say the first 2048 events in *event_numbers*, one could

.. code-block:: python
   :linenos:

   import sqlite3

   def extract_all_event_numbers(db):
      with sqlite3.connect(db) as con:
         query = 'from truth select event_no'
         event_numbers = pd.read_sql(query, con)
      return event_numbers

   db = 'path_to_database'
   event_numbers = extract_all_event_numbers(db)

   our_events_of_interest = event_numbers['event_no][0:2048]
   with sqlite3.connect(db) as con:
      query = 'from SRTInIcePulses select * where event_no in '%str(tuple(our_events_of_interest))
      pulses = pd.read_sql(query, con)


The standard truth extraction:
========
Once the database is created, it will have a table named **truth**. Within the table, the following columns will be available:

.. list-table:: Standard Truth Extraction
   :widths: 25 25
   :header-rows: 1

   * - Truth Column
     - IceTray Syntax
   * - energy
     - MCInIcePrimary.energy
   * - position_x
     - MCInIcePrimary.pos.x
   * - position_y
     - MCInIcePrimary.pos.y
   * - position_z
     - MCInIcePrimary.pos.z
   * - azimuth
     - MCInIcePrimary.dir.azimuth
   * - zenith
     - MCInIcePrimary.dir.zenith
   * - pid
     - MCInIcePrimary.pdg_encoding
   * - event_time
     - frame['I3EventHeader'].start_time.utc_daq_time
   * - sim_type
     - genie, muongun, corsika, noise or data 
   * - interaction_type
     - frame["I3MCWeightDict"]["InteractionType"]
   * - elasticity
     - frame['I3GENIEResultDict']['y']
   * - RunID
     - frame['I3EventHeader'].run_id
   * - SubrunID
     - frame['I3EventHeader'].sub_run_id
   * - EventID
     - frame['I3EventHeader'].event_id
   * - SubEventID
     - frame['I3EventHeader'].sub_event_id

**Note**: Future version will allow you to specify the truth columns explicitly in the configuration. 

The standard pulse extraction:
========
Once the database is created, it will have a table named after each of the elements in *pulse_keys*. E.g., if you pick 'SRTInIcePulses', a table in the database will carry this name. Within the table, the following columns will be available:

.. list-table:: Standard Pulse Extraction
   :widths: 25 25
   :header-rows: 1

   * - Pulse Column
     - IceTray 
   * - charge
     - pulse.charge
   * - dom_time
     - pulse.time
   * - dom_x
     - gcd_dict[om_key].position.x
   * - dom_y
     - gcd_dict[om_key].position.y
   * - dom_z
     - gcd_dict[om_key].position.z
   * - width
     - pulse.width
   * - pmt_area
     - gcd_dict[om_key].area
   * - rde 
     - frame["I3Calibration"].dom_cal[om_key].relative_dom_eff

Here *pulse* corresponds to, in the case where *SRTInIcePulses* is the pulse key: 

.. code-block:: python

   for om_key in om_keys:
      pulses = frame['SRTInIcePulses'][om_key]

and *om_key* is the OM index available in the i3 file directly. The OM index matches pulses in i3 files with geospatial information contained in the GCD-file, which is represented here as *om_dict*.


**Note**: Future version will allow you to specify the pulse columns explicitly in the configuration.

The standard RetroReco extraction:
========
Once the database is created, it will have a table named RetroReco, which contains reconstructions from RetroReco and other associated variables with the OscNext Filtering:

.. list-table:: Standard RetroReco Extraction
   :widths: 25 25
   :header-rows: 1

   * - RetroReco Column
     - IceTray 
   * - azimuth_retro
     - frame["L7_reconstructed_azimuth"].value
   * - zenith_retro
     - frame["L7_reconstructed_zenith"].value
   * - time_retro
     - frame["L7_reconstructed_time"].value
   * - energy_retro
     - frame["L7_reconstructed_total_energy"].value
   * - cascade_energy_retro
     - frame["L7_reconstructed_cascade_energy"].value
   * - track_energy_retro
     - frame["L7_reconstructed_track_energy"].value
   * - track_length_retro
     - frame["L7_reconstructed_track_length"].value
   * - position_x_retro
     - frame["L7_reconstructed_vertex_x"].value
   * - position_y_retro
     - frame["L7_reconstructed_vertex_y"].value
   * - position_z_retro
     - frame["L7_reconstructed_vertex_z"].value
   * - lvl7_probnu
     - frame["L7_MuonClassifier_FullSky_ProbNu"].value
   * - lvl7_prob_track
     - frame["L7_PIDClassifier_FullSky_ProbTrack"].value
   * - lvl4_probnu
     - frame["L4_MuonClassifier_Data_ProbNu"].value
   * - azimuth_sigma
     - frame["L7_retro_crs_prefit__azimuth_sigma_tot"].value
   * - zenith_sigma
     - frame["L7_retro_crs_prefit__zenith_sigma_tot"].value
   * - energy_sigma
     - frame["L7_retro_crs_prefit__energy_sigma_tot"].value
   * - time_sigma
     - frame["L7_retro_crs_prefit__time_sigma_tot"].value
   * - position_x_sigma
     - frame["L7_retro_crs_prefit__x_sigma_tot"].value
   * - position_y_sigma
     - frame["L7_retro_crs_prefit__y_sigma_tot"].value
   * - position_z_sigma
     - frame["L7_retro_crs_prefit__z_sigma_tot"].value
   * - osc_weight
     - frame["I3MCWeightDict"]["weight"]*
                        
* the osc_weight is only added if the i3files contains simulated data.  
   

**Note**: Future version will allow you to specify the RetroReco columns explicitly in the configuration.

FAQ:
========
* My pulse map table is empty - why is that?
   *This is likely because the pulse map is not present in the i3 files you specified. Check for typos!*

* I'd like to modify the truth variables selected for extraction, how do I do this?
   *This is currently not possible, but will be added as a feature in the next update.*

* I'd like to modify the RetroReco variables selected for extraction, how do I do this?
   *This is currently not possible, but will be added as a feature in the next update.*

* I'd like to modify the pulse map variables selected for extraction, how do I do this?
   *This is currently not possible, but will be added as a feature in the next update.*   

* What can I do to optimize database creation time?

  *The biggest contributing factor is the amount of pulse map keys that you select for extraction. Also, because the uncleaned pulses contain more pulses, these typically takes a longer time to extract. Additionally, because the i3-files are simply distributed evenly amongst the cores, without taking into account the amount of PFrames in each, its possible that sometimes 90% of the cores will finish early while the remaining is working through larger i3 files.*

Contribute
========

- Source Code: https://github.com/RasmusOrsoe/I3ToSQLite/

Support
========

Join IceCube Slack Channel #I3ToSQLite for help 

License
========

The project is licensed under the MIT license.