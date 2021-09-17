import os
import pickle
import I3ToSQLite

def Build_Configuration(paths, outdir, workers, pulse_keys, db_name, gcd_rescue, verbose):
    dictionary = {'paths': paths,
                  'outdir': outdir,
                  'workers': workers,
                  'pulse_keys': pulse_keys,
                  'db_name': db_name,
                  'gcd_rescue': gcd_rescue,
                  'verbose': verbose}
    dictionary_path = outdir + '/%s/config'%db_name
    try:
        os.makedirs(dictionary_path)
    except:
        notimportant = 0

    with open(dictionary_path + '/config.pkl', 'wb') as handle:
        pickle.dump(dictionary, handle, protocol=2) 
    
    print(dictionary_path)              
    return dictionary_path
def MakeDir(coms_path):
    try:
        os.makedirs(coms_path)
    except:
        notimportant = 0
    return coms_path

def Write_Handler(cvmfs_setup_path, cvmfs_shell_path, coms_path):
    CODE = "eval `%s` \n%s %s/run_extraction.sh"%(cvmfs_setup_path,cvmfs_shell_path, coms_path)
    text_file = open("%s/handler.sh"%coms_path, "w")
    text_file.write(CODE)
    text_file.close()
    os.system("chmod 755 %s/handler.sh"%coms_path)

def Write_Executer(config_path, coms_path):
    directory_path = os.path.dirname(I3ToSQLite.__file__)
    code = "python {0}/CreateTemporaryDatabases.py --config {1} && python {0}/MergeTemporaryDatabases.py --config {1} && exit".format(directory_path, config_path)
    text_file = open("%s/run_extraction.sh"%coms_path, "w")
    text_file.write(code)
    text_file.close()
    os.system("chmod 755 %s/run_extraction.sh"%coms_path)

def CreateDatabase(paths, outdir, workers, cvmfs_setup_path, cvmfs_shell_path, db_name, pulse_keys, gcd_rescue, verbose = 1):
    config_path = Build_Configuration(paths, outdir, workers, pulse_keys,db_name, gcd_rescue, verbose)
    coms_path = MakeDir(outdir + '/%s/config'%db_name)
    Write_Executer(config_path, coms_path)
    Write_Handler(cvmfs_setup_path, cvmfs_shell_path, coms_path)
    os.system('%s/handler.sh'%coms_path)

