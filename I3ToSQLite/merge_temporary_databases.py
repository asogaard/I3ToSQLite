import os
from sqlalchemy import create_engine
import sqlalchemy
import pandas as pd
import sqlite3
import time
import pickle
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-config", "--config", type=str, required=True)


def fetch_temps(path):
    out = []
    files = os.listdir(path)
    for file in files:
        if '.db' in file:
            out.append(file)
    return out

def extract_everything(db, keys):
    pulse_maps = {}
    with sqlite3.connect(db) as con:
        truth_query = 'select * from truth'
        truth = pd.read_sql(truth_query,con)
    for key in keys:
        with sqlite3.connect(db) as con:
            pulse_map_query = 'select * from %s'%key
            pulse_map = pd.read_sql(pulse_map_query,con)
        pulse_maps[key] = pulse_map
    try:
        with sqlite3.connect(db) as con:
            retro_query = 'select * from RetroReco'
            retro = pd.read_sql(retro_query,con)
    except:
        retro = []
    return truth, pulse_maps, retro

def extract_column_names(tmp_path, db_files, keys):
    pulse_map_columns = {}
    db = tmp_path + '/' + db_files[0]
    with sqlite3.connect(db) as con:
        truth_query = 'select * from truth limit 1'
        truth_columns = pd.read_sql(truth_query,con).columns
    for key in keys:
        with sqlite3.connect(db) as con:
            pulse_map_query = 'select * from %s limit 1'%key
            pulse_map = pd.read_sql(pulse_map_query,con)
        pulse_map_columns[key] = pulse_map.columns
    
    for db_file in db_files:
        db = tmp_path + '/' + db_file
        try:
            with sqlite3.connect(db) as con:
                retro_query = 'select * from RetroReco limit 1'
                retro_columns = pd.read_sql(retro_query,con).columns
            if len(retro_columns)>0:
                break
        except:
            retro_columns = []
        


    return truth_columns, pulse_map_columns, retro_columns

def run_sql_code(database, CODE):
    conn = sqlite3.connect(database + '.db')
    c = conn.cursor()
    c.executescript(CODE)
    c.close()  
    return

def attach_index(database, table_name):
    CODE = "PRAGMA foreign_keys=off;\nBEGIN TRANSACTION;\nCREATE INDEX event_no_{} ON {} (event_no);\nCOMMIT TRANSACTION;\nPRAGMA foreign_keys=on;".format(table_name,table_name)
    run_sql_code(database,CODE)
    return

def create_table(database,table_name, columns, is_pulse_map = False):
    count = 0
    for column in columns:
        if count == 0:
            if column == 'event_no':
                if is_pulse_map == False:
                    query_columns =  column + ' INTEGER PRIMARY KEY NOT NULL'
                else:
                     query_columns =  column + ' NOT NULL'
            else: 
                query_columns =  column + ' FLOAT'
        else:
            if column == "event_no":
                if is_pulse_map == False:
                    query_columns =  query_columns + ', ' + column + ' INTEGER PRIMARY KEY NOT NULL'
                else:
                     query_columns = query_columns + ', '+ column + ' NOT NULL' 
            else:
                query_columns = query_columns + ', ' + column + ' FLOAT'

        count +=1
    CODE = "PRAGMA foreign_keys=off;\nCREATE TABLE {} ({});\nPRAGMA foreign_keys=on;".format(table_name,query_columns) 
    run_sql_code(database, CODE)
    if is_pulse_map:
        #try:
        print(table_name)
        print('attaching indexs')
        attach_index(database,table_name)
        #except:
        #    notimportant = 0
    return

def create_empty_tables(database,pulse_map_keys,truth_columns, pulse_map_columns, retro_columns):
    
    print('Creating Empty Truth Table')
    create_table(database, 'truth', truth_columns, is_pulse_map = False) # Creates the truth table containing primary particle attributes and RetroReco reconstructions
    print('Creating Empty RetroReco Table')
    create_table(database, 'RetroReco',retro_columns, is_pulse_map = False) # Creates the RetroReco Table with reconstuctions and associated values.
    
    for pulse_map_key in pulse_map_keys:
        # Creates the pulse map tables
        print('Creating Empty %s Table'%pulse_map_key)
        create_table(database, pulse_map_key,pulse_map_columns[pulse_map_key], is_pulse_map = True)
    return

def submit_truth(database, truth):
    engine_main = sqlalchemy.create_engine('sqlite:///' + database + '.db')
    truth.to_sql('truth',engine_main,index= False, if_exists = 'append')
    engine_main.dispose()
    return  

def submit_pulse_maps(database, pulse_maps):
    engine_main = sqlalchemy.create_engine('sqlite:///' + database + '.db')
    for pulse_map in pulse_maps.keys():
        pulse_maps[pulse_map].to_sql(pulse_map, engine_main,index= False, if_exists = 'append')
    engine_main.dispose()
    return

def submit_retro(database, retro):
    if len(retro)>0:
        engine_main = sqlalchemy.create_engine('sqlite:///' + database + '.db')
        retro.to_sql('RetroReco',engine_main,index= False, if_exists = 'append')
        engine_main.dispose()
    return  

def merge_temporary_databases(database, db_files, path_to_tmp,pulse_map_keys):
    file_counter = 1
    print_message()
    for i in tqdm(range(len(db_files)), colour = 'green'):
        file = db_files[i]
        truth, pulse_maps, retro = extract_everything(path_to_tmp + '/'  + file,pulse_map_keys)
        submit_truth(database,truth)
        submit_pulse_maps(database, pulse_maps)
        submit_retro(database, retro)
        file_counter += 1
    return

def pickle_cleaner(List):
    clean_list = []
    for element in List:
        clean_list.append(str(element))
    return clean_list

def extract_config(config_path):
    with open('%s/config.pkl'%config_path, 'rb') as handle:
        config = pickle.load(handle)  
    paths = pickle_cleaner(config['paths'])
    
    outdir = str(config['outdir'])
    workers = int(config['workers'])

    pulse_keys = pickle_cleaner(config['pulse_keys'])
   
    db_name = str(config['db_name'])

    start_time = float(config['start_time'])
    try:
        max_dictionary_size = int(config['max_dictionary_size'])
    except:
        max_dictionary_size = 10000
    try:
        custom_truth    =   pickle_cleaner(config['custom_truth'])
        
    except:
        custom_truth    = None
    
    return paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth, start_time

def create_directory(dir):
    try:
        os.makedirs(dir)
        return False
    except:
        return True

def print_message():
    print('-----------------')
    print('MERGING DATABASES')
    print('-----------------')

def create_database(database_name,outdir, pulse_map_keys):
    path_tmp = outdir + '/' + database_name + '/tmp'
    database_path = outdir + '/' + database_name + '/' + database_name
    directory_exists = create_directory(outdir)
    db_files = fetch_temps(path_tmp)
    print('Found %s .db-files in %s'%(len(db_files),path_tmp))
    truth_columns, pulse_map_columns, retro_columns = extract_column_names(path_tmp, db_files, pulse_map_keys)
    create_empty_tables(database_path,pulse_map_keys, truth_columns, pulse_map_columns, retro_columns)
    merge_temporary_databases(database_path, db_files, path_tmp, pulse_map_keys)
    os.system('rm -r %s'%path_tmp)
    return


# Main function definition
def main ():
    args = parser.parse_args()

    paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth, start_time = extract_config(args.config)
    create_database(db_name, outdir, pulse_keys)

    print('Database Creation Successful!')
    print('Time Elapsed: %s minutes'%((time.time() - start_time)/60))


# Main function call
if __name__ == '__main__':
    main()