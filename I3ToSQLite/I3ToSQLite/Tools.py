import os
import pickle
from I3ToSQLite import anchor


def Build_Configuration(paths, outdir, workers, pulse_keys, db_name, gcd_rescue):
    dictionary = {'paths': paths,
                  'outdir': outdir,
                  'workers': workers,
                  'pulse_keys': pulse_keys,
                  'db_name': db_name,
                  'gcd_rescue': gcd_rescue}
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
def MakeDir():
    try:
        os.makedirs('tmp/coms')
    except:
        notimportant = 0
def Write_Handler(cvmfs_setup_path, cvmfs_shell_path):
    CODE = "eval `%s` \n%s ./tmp/coms/run_extraction.sh"%(cvmfs_setup_path,cvmfs_shell_path)
    text_file = open("tmp/coms/handler.sh", "w")
    text_file.write(CODE)
    text_file.close()
    os.system("chmod 755 tmp/coms/handler.sh")

def Write_Executer():
    path = (anchor.__file__).split('anchor.py')[0]
    CODE = "python %sCreateTemporaryDatabases.py && python %sMergeTemporaryDatabases.py && exit"%(path,path)
    text_file = open("tmp/coms/run_extraction.sh", "w")
    text_file.write(CODE)
    text_file.close()
    os.system("chmod 755 tmp/coms/run_extraction.sh")

def CreateDatabase(paths, outdir, workers, cvmfs_setup_path, cvmfs_shell_path, db_name, pulse_keys, gcd_rescue):
    configuration_path = Build_Configuration(paths, outdir, workers, pulse_keys,db_name, gcd_rescue)
    MakeDir()
    Write_Executer()
    Write_Handler(cvmfs_setup_path, cvmfs_shell_path)
    os.system('./tmp/coms/handler.sh')



