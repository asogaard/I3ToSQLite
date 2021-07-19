import os
import pickle


def Build_Configuration(paths, outdir, workers, pulse_keys, db_name):
    dictionary = {'paths': paths,
                  'outdir': outdir,
                  'workers': workers,
                  'pulse_keys': pulse_keys,
                  'db_name': db_name + '.db'}
    dictionary_path = outdir + '/%s/config'%db_name
    try:
        os.makedirs(dictionary_path)
    except:
        notimportant = 0
    try:
        os.makedirs('tmp/config')
    except:
        notimportant = 0

    with open(dictionary_path + '/config.pkl', 'wb') as handle:
        pickle.dump(dictionary, handle, protocol=2) 
    with open('tmp/config/config.pkl', 'wb') as handle:
        pickle.dump(dictionary, handle, protocol=2)              
    return dictionary_path + '/config.pkl'

def Write_Shell_Script(cvmfs_setup_path, cvmfs_shell_path):
    CODE = "eval `%s` \n%s ./run_extraction.sh"%(cvmfs_setup_path,cvmfs_shell_path)
    text_file = open("handler_test.sh", "w")
    text_file.write(CODE)
    text_file.close()
    os.system("chmod 755 handler_test.sh")
    
def CreateDatabase(paths, outdir, workers, cvmfs_setup_path, cvmfs_shell_path, db_name, pulse_keys):
    configuration_path = Build_Configuration(paths, outdir, workers, pulse_keys,db_name)
    Write_Shell_Script(cvmfs_setup_path, cvmfs_shell_path)
    os.system('./handler_test.sh')


paths       = ['/groups/hep/pcs557/i3_workspace/test_i3/noise',
                '/groups/hep/pcs557/i3_workspace/test_i3/genie',
                '/groups/hep/pcs557/i3_workspace/test_i3/muongun']

outdir      = '/groups/hep/pcs557/i3_workspace/test_i3/test_db'
workers     = 5
cvmfs_setup_path  = '/cvmfs/icecube.opensciencegrid.org/py2-v3.1.1/setup.sh'
cvmfs_shell_path = '/cvmfs/icecube.opensciencegrid.org/users/Oscillation/software/oscNext_meta/releases/latest/build/env-shell.sh'
db_name     = 'all_keys_test'
pulse_keys  = ['SRTInIcePulses',
                'SRTTWOfflinePulsesDC',
                'SplitInIceDSTPulses',
                'SplitInIcePulses',
                'TWOfflinePulsesDC',
                'L5_SANTA_DirectPulses',
                'InIcePulses',
                'IceTopDSTPulses',
                'IceTopPulses']
#pulse_keys  = ['SRTInIcePulses',
#                'SplitInIcePulses']

