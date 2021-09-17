"""Minimum working example for converting I3 files to a SQLite database.

This script ...
"""

# Import(s)
from os.path import expanduser
from I3ToSQLite.Tools import CreateDatabase

# Global variable(s)
CVMFS = "/cvmfs/icecube.opensciencegrid.org"

# Main function definition
def main():
    """Script to create SQLite database from I3 file(s)."""

    paths = [expanduser("~/data/i3/i3_to_sqlite_workshop_test/level7_v02.00")]
    db_name = "my_database"
    outdir = expanduser("~/data/sqlite")
    workers = 42
    cvmfs_setup_path = "{}/py3-v4/setup.sh".format(CVMFS)
    cvmfs_shell_path = "{}/users/Oscillation/software/oscNext_meta/releases/latest/build/env-shell.sh".format(CVMFS)  # pylint: disable=line-too-long
    gcd_rescue = expanduser("~/gnn/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withScaledNoise.i3.gz")  # pylint: disable=line-too-long
    pulse_keys = ["SRTInIcePulses"]

    CreateDatabase(
        paths,
        outdir,
        workers,
        cvmfs_setup_path,
        cvmfs_shell_path,
        db_name,
        pulse_keys,
        gcd_rescue,
        verbose=1,
    )

# Main function call
if __name__ == "__main__":
    main()
