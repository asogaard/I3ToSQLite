'''

'''
import os, glob
try:
    from icecube import dataclasses, icetray, dataio
    from icecube import genie_icetray
except ModuleNotFoundError:
    # Not running in IceTray
    pass
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import time
from multiprocessing import Pool
import pickle
import argparse
import sys

parser = argparse.ArgumentParser()
parser.add_argument("-config", "--config", type=str, required=True)



def contains_retro(frame):
    try:
        frame['L7_reconstructed_zenith']
        return True
    except:
        return False

def build_standard_extraction():
    standard_truths = {'energy': 'MCInIcePrimary.energy',
            'position_x': 'MCInIcePrimary.pos.x', 
            'position_y': 'MCInIcePrimary.pos.y', 
            'position_z': 'MCInIcePrimary.pos.z',
            'azimuth': 'MCInIcePrimary.dir.azimuth',
            'zenith': 'MCInIcePrimary.dir.zenith',
            'pid': 'MCInIcePrimary.pdg_encoding',
            'event_time': 'event_time',
            'sim_type': 'sim_type',
            'interaction_type': 'interaction_type',
            'elasticity': 'elasticity',
            'RunID': 'RunID',
            'SubrunID': 'SubrunID',
            'EventID': 'EventID',
            'SubEventID': 'SubEventID'}
    return standard_truths

def case_handle_this(frame, sim_type):
    if sim_type != 'noise':
        MCInIcePrimary = frame['MCInIcePrimary']
    else:
        MCInIcePrimary = None
    if sim_type != 'muongun' and sim_type != 'noise':
        interaction_type =  frame["I3MCWeightDict"]["InteractionType"]
        elasticity = frame['I3GENIEResultDict']['y']
    else:
        interaction_type = -1
        elasticity = -1
    return MCInIcePrimary, interaction_type, elasticity

def is_montecarlo(frame):
    mc = True
    try:
        frame['MCInIcePrimary']
    except:
        mc = False
    return mc

def build_blank_extraction():
    ## Please note that if the simulation type is pure noise or real that these values will be appended to the truth table
    blank_extraction = {'energy_log10': '-1',
            'position_x': '-1', 
            'position_y': '-1', 
            'position_z': '-1',
            'azimuth': '-1',
            'zenith': '-1',
            'pid': '-1',
            'event_time': 'event_time',
            'sim_type': 'sim_type',
            'interaction_type': '-1',
            'elasticity': '-1',
            'RunID': 'RunID',
            'SubrunID': 'SubrunID',
            'EventID': 'EventID',
            'SubEventID': 'SubEventID'}
    return blank_extraction

def build_retro_extraction(is_mc):
    retro_extraction = {'azimuth_retro': 'frame["L7_reconstructed_azimuth"].value',
                        'time_retro': 'frame["L7_reconstructed_time"].value',
                        'energy_retro': 'frame["L7_reconstructed_total_energy"].value', 
                        'position_x_retro': 'frame["L7_reconstructed_vertex_x"].value', 
                        'position_y_retro': 'frame["L7_reconstructed_vertex_y"].value',
                        'position_z_retro': 'frame["L7_reconstructed_vertex_z"].value',
                        'zenith_retro': 'frame["L7_reconstructed_zenith"].value',
                        'azimuth_sigma': 'frame["L7_retro_crs_prefit__azimuth_sigma_tot"].value',
                        'position_x_sigma': 'frame["L7_retro_crs_prefit__x_sigma_tot"].value',
                        'position_y_sigma': 'frame["L7_retro_crs_prefit__y_sigma_tot"].value',
                        'position_z_sigma': 'frame["L7_retro_crs_prefit__z_sigma_tot"].value',
                        'time_sigma': 'frame["L7_retro_crs_prefit__time_sigma_tot"].value',
                        'zenith_sigma': 'frame["L7_retro_crs_prefit__zenith_sigma_tot"].value',
                        'energy_sigma': 'frame["L7_retro_crs_prefit__energy_sigma_tot"].value',
                        'cascade_energy_retro': 'frame["L7_reconstructed_cascade_energy"].value',
                        'track_energy_retro': 'frame["L7_reconstructed_track_energy"].value',
                        'track_length_retro': 'frame["L7_reconstructed_track_length"].value',
                        'lvl7_probnu': 'frame["L7_MuonClassifier_FullSky_ProbNu"].value',
                        'lvl4_probnu': 'frame["L4_MuonClassifier_Data_ProbNu"].value',
                        'lvl7_prob_track': 'frame["L7_PIDClassifier_FullSky_ProbTrack"].value'}

    if is_mc:
        retro_extraction['osc_weight'] = 'frame["I3MCWeightDict"]["weight"]'    
    return retro_extraction

def extract_retro(frame):
    is_mc = is_montecarlo(frame)
    retro = {}
    if contains_retro(frame):
        retro_extraction = build_retro_extraction(is_mc)
        for retro_variable in retro_extraction.keys():
            retro[retro_variable] = eval(retro_extraction[retro_variable]) 
    return retro

def extract_truth(frame, input_file, extract_these_truths = None):
    if extract_these_truths == None:
        extract_these_truths = build_standard_extraction()
    is_mc = is_montecarlo(frame)
    sim_type = find_simulation_type(is_mc,input_file)
    event_time =  frame['I3EventHeader'].start_time.utc_daq_time
    RunID, SubrunID, EventID, SubEventID = extract_event_ids(frame)
    if is_mc:
        MCInIcePrimary, interaction_type, elasticity = case_handle_this(frame, sim_type)
        if MCInIcePrimary != None:
            ## is not noise
            truth = {}
            for truth_variable in extract_these_truths.keys():
                truth[truth_variable] = eval(extract_these_truths[truth_variable])
    else:
        ## is real data or noise   
        blank_extraction = build_blank_extraction()
        truth = {}
        for truth_variable in blank_extraction.keys():
            truth[truth_variable] = eval(blank_extraction[truth_variable])
    return truth

def extract_features(frame, key, gcd_dict,calibration):
    charge = []
    time   = []
    width  = []
    area   = []
    rqe    = []
    x       = []
    y       = []
    z       = []
    if key in frame.keys():
        data    = frame[key]
        try:
            om_keys = data.keys()
        except:
            try:
                if "I3Calibration" in frame.keys():
                    data = frame[key].apply(frame)
                    om_keys = data.keys()
                else:
                    frame["I3Calibration"] = calibration 
                    data = frame[key].apply(frame)
                    om_keys = data.keys()
            except:
                data = dataclasses.I3RecoPulseSeriesMap.from_frame(frame,key)
                om_keys = data.keys()
        for om_key in om_keys:
            pulses = data[om_key]
            for pulse in pulses:
                charge.append(pulse.charge)
                time.append(pulse.time) 
                width.append(pulse.width)
                area.append(gcd_dict[om_key].area)  
                rqe.append(frame["I3Calibration"].dom_cal[om_key].relative_dom_eff)
                x.append(gcd_dict[om_key].position.x)
                y.append(gcd_dict[om_key].position.y)
                z.append(gcd_dict[om_key].position.z)
        
    features = {'charge': charge, 
                'dom_time': time, 
                'dom_x': x, 
                'dom_y': y, 
                'dom_z': z,
                'width' : width,
                'pmt_area': area, 
                'rde': rqe}
    return features
    

def find_simulation_type(mc, input_file):
    if mc == False:
        sim_type = 'data'
    else:
        sim_type = 'lol'
    if 'muon' in input_file:
        sim_type = 'muongun'
    if 'corsika' in input_file:
        sim_type = 'corsika'
    if 'genie' in input_file:
        sim_type = 'genie'
    if 'noise' in input_file:
        sim_type = 'noise'
    if sim_type == 'lol':
        print('SIM TYPE NOT FOUND!')
    return sim_type

def load_geospatial_data(gcd_path):
    gcd_file = dataio.I3File(gcd_path)
    g_frame = gcd_file.pop_frame(icetray.I3Frame.Geometry)
    om_geom_dict = g_frame["I3Geometry"].omgeo
    calibration = gcd_file.pop_frame(icetray.I3Frame.Calibration)["I3Calibration"]
    return om_geom_dict, calibration  

def is_empty(features):
    if features['dom_x'] != None:
        return False
    else:
        return True

def extract_event_ids(frame):
    RunID       = frame['I3EventHeader'].run_id
    SubrunID    = frame['I3EventHeader'].sub_run_id
    EventID     = frame['I3EventHeader'].event_id
    SubEventID  = frame['I3EventHeader'].sub_event_id
    return RunID, SubrunID, EventID, SubEventID

def apply_event_no(extraction, event_no_list, event_counter):
    out = pd.DataFrame(extraction.values()).T
    out.columns = extraction.keys()
    out['event_no'] = event_no_list[event_counter]
    return out

def check_for_new_columns(columns, biggest_columns):
    if len(columns) > len(biggest_columns):
        return columns
    else:
        return biggest_columns

def write_dicts(settings):
    input_files,id,gcd_files,outdir , max_dict_size,event_no_list, pulse_map_keys,custom_truth, db_name,verbose = settings
    # Useful bits
    event_counter = 0
    feature_big = {}
    truth_big   = pd.DataFrame()
    retro_big   = pd.DataFrame()
    file_counter = 0
    output_count = 0
    gcd_count = 0
    for u in range(len(input_files)):
        input_file = input_files[u]
        gcd_dict, calibration = load_geospatial_data(gcd_files[u])
        i3_file = dataio.I3File(input_file, "r")
        if verbose > 0:
            print('Worker %s Reading %s'%(id,input_file.split('/')[-1]))
            print(input_file)
            sys.stdout.flush()
        gcd_count  +=1
    
        while i3_file.more() :
            try:
                frame = i3_file.pop_physics()
            except:
                frame = False
            if frame :
                pulse_maps = {}
                for pulse_map_key in pulse_map_keys:
                    pulse_maps[pulse_map_key] = extract_features(frame,pulse_map_key, gcd_dict,calibration)
                   
                truths   = extract_truth(frame, input_file, custom_truth)
                truth    = apply_event_no(truths, event_no_list, event_counter)
                retros   = extract_retro(frame)
                if len(retros)>0:
                    retro   = apply_event_no(retros, event_no_list, event_counter)

                for pulse_map_key in pulse_map_keys:
                    if not is_empty(pulse_maps[pulse_map_key]) :
                        pulse_maps[pulse_map_key] = apply_event_no(pulse_maps[pulse_map_key], event_no_list, event_counter)
                event_counter += 1
                if len(feature_big) == 0:
                    feature_big = pulse_maps
                else:
                    for pulse_map_key in pulse_map_keys:
                        feature_big[pulse_map_key] = feature_big[pulse_map_key].append(pulse_maps[pulse_map_key],ignore_index = True, sort = True)
                truth_big   = truth_big.append(truth, ignore_index = True, sort = True)
                
                if len(retros)>0 :
                    retro_big   = retro_big.append(retro, ignore_index = True, sort = True)
                if len(truth_big) >= max_dict_size:
                    print('saving')
                    engine = sqlalchemy.create_engine('sqlite:///'+outdir + '/%s/tmp/worker-%s-%s.db'%(db_name,id,output_count))
                    truth_big.to_sql('truth',engine,index= False, if_exists = 'append')
                    if len(retro_big)> 0:
                        retro_big.to_sql('RetroReco',engine,index= False, if_exists = 'append')
                    for pulse_map_key in pulse_map_keys:
                        feature_big[pulse_map_key].to_sql(pulse_map_key,engine,index= False, if_exists = 'append')
                    engine.dispose()
                    feature_big = {} #pd.DataFrame()
                    truth_big   = pd.DataFrame()
                    retro_big   = pd.DataFrame()
                    output_count +=1
        file_counter +=1
        if verbose > 0:
            print('Worker %s has finished %s/%s I3 files.'%(id, file_counter, len(input_files)))
    if (len(feature_big) > 0):
        print('saving eof')
        engine = sqlalchemy.create_engine('sqlite:///'+outdir +  '/%s/tmp/worker-%s-%s.db'%(db_name,id,output_count))
        truth_big.to_sql('truth',engine,index= False, if_exists = 'append')
        if len(retro_big)> 0:
            retro_big.to_sql('RetroReco',engine,index= False, if_exists = 'append')
        for pulse_map_key in pulse_map_keys:
            feature_big[pulse_map_key].to_sql(pulse_map_key,engine,index= False, if_exists = 'append')
        engine.dispose()
        feature_big = {} 
        truth_big   = pd.DataFrame()
        retro_big   = pd.DataFrame()
    

def is_i3(file):
    if 'gcd' in file.lower():
        return False
    elif 'geo' in file.lower():
        return False
    else:
        return True

def has_extension(file, extensions):
    check = 0
    for extension in extensions:
        if extension in file:
            check +=1
    if check >0:
        return True
    else:
        return False

def walk_directory(dir, extensions, gcd_rescue):
    files_list = []
    GCD_list   = []
    root,folders,root_files = next(os.walk(dir))
    gcds_root = []
    gcd_root = None
    i3files_root = []
    for file in root_files:
        if has_extension(file, extensions):
            if is_i3(file):
                i3files_root.append(os.path.join(root,file))
            else:
                gcd_root = os.path.join(root,file)
                gcds_root.append(os.path.join(root,file))
    if gcd_root == None:
        gcd_root = gcd_rescue
    for k in range(len(i3files_root) - len(gcds_root)):
        gcds_root.append(gcd_root)
    files_list.extend(i3files_root)
    GCD_list.extend(gcds_root)
    for folder in folders:
        sub_root, sub_folders, sub_folder_files = next(os.walk(os.path.join(root,folder)))
        gcds_folder = []
        gcd_folder = None
        i3files_folder = []
        for sub_folder_file in sub_folder_files:
            if has_extension(sub_folder_file, extensions):
                if is_i3(sub_folder_file):
                    i3files_folder.append(os.path.join(sub_root,sub_folder_file))
                else:
                    gcd_folder = os.path.join(sub_root,sub_folder_file)
                    gcds_folder.append(os.path.join(sub_root,sub_folder_file))
        if gcd_folder == None:
            gcd_folder = gcd_rescue
        for k in range(len(i3files_folder) - len(gcds_folder)):
            gcds_folder.append(gcd_folder)
        files_list.extend(i3files_folder)
        GCD_list.extend(gcds_folder)
    
    files_list, GCD_list = pairwise_shuffle(files_list, GCD_list)
    return files_list, GCD_list

def pairwise_shuffle(files_list, gcd_list):
    df = pd.DataFrame({'i3': files_list, 'gcd': gcd_list})
    df_shuffled = df.sample(frac = 1)
    i3_shuffled = df_shuffled['i3'].tolist()
    gcd_shuffled = df_shuffled['gcd'].tolist()
    return i3_shuffled, gcd_shuffled


def find_files(paths,outdir,db_name,gcd_rescue, extensions = None):
    print('Counting files in: \n%s\n This might take a few minutes...'%paths)
    if extensions == None:
        extensions = ("i3.bz2",".zst",".gz")
    input_files_mid = []
    input_files = []
    files = []
    gcd_files_mid = []
    gcd_files = []
    for path in paths:
        input_files_mid, gcd_files_mid = walk_directory(path, extensions, gcd_rescue)
        input_files.extend(input_files_mid)
        gcd_files.extend(gcd_files_mid)

    input_files, gcd_files = pairwise_shuffle(input_files, gcd_files)
    save_filenames(input_files, outdir, db_name)
    return input_files, gcd_files
    
def save_filenames(input_files,outdir, db_name):
    input_files = pd.DataFrame(input_files)
    input_files.columns = ['filename']
    input_files.to_csv(outdir + '/%s/config/i3files.csv'%db_name)
    return

def create_out_directory(outdir):
    try:
        os.makedirs(outdir)
        return False
    except:
        print(' !!WARNING!! \n \
            %s already exists. \n \
            ABORTING! '%outdir)
        return True

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
    gcd_rescue = str(config['gcd_rescue'])
    verbose = int(config['verbose'])
    try:
        max_dictionary_size = int(config['max_dictionary_size'])
    except:
        max_dictionary_size = 10000
    try:
        custom_truth    =   pickle_cleaner(config['custom_truth'])
        
    except:
        custom_truth    = None
    return paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth, gcd_rescue, verbose

def transmit_start_time(start_time,config_path):
    with open('%s/config.pkl'%config_path, 'rb') as handle:
        config = pickle.load(handle)
    config['start_time'] = start_time
    with open('%s/config.pkl'%config_path, 'wb') as handle:
        pickle.dump(config, handle, protocol=2)  
    return

def print_message(workers, input_files):
    print('Found %s I3 files!'%len(input_files))
    print('----------------------')
    print('READING FILES IN %s PARALLEL SESSIONS'%workers)
    print('----------------------')

def create_temporary_databases(paths, outdir, workers, pulse_keys,config_path, start_time,db_name,gcd_rescue,verbose,max_dictionary_size = 10000, custom_truth = None):
    if __name__ == "__main__" :
        start_time = time.time()
        if verbose == 0:
            icetray.I3Logger.global_logger = icetray.I3NullLogger()    
        directory_exists = create_out_directory(outdir + '/%s/tmp'%db_name)
        input_files, gcd_files = find_files(paths, outdir,db_name,gcd_rescue)
        if workers > len(input_files):
            workers = len(input_files)
        
        # SETTINGS
        settings = []
        event_nos = np.array_split(np.arange(0,99999999,1),workers) #Notice that this choice means event_no is NOT unique between different databases.
        file_list = np.array_split(np.array(input_files),workers)
        gcd_file_list = np.array_split(np.array(gcd_files),workers)

        for i in range(0,workers):
            settings.append([file_list[i],str(i),gcd_file_list[i],outdir,max_dictionary_size,event_nos[i], pulse_keys, custom_truth, db_name, verbose])
        #WriteDicts(settings[0])
        print_message(workers, input_files)       
        p = Pool(processes = workers)
        p.map_async(write_dicts, settings)
        p.close()
        p.join()
        print('transmitting..')
        transmit_start_time(start_time, config_path)


# Main function definition
def main ():
    args = parser.parse_args()

    start_time = time.time()

    paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth, gcd_rescue, verbose = extract_config(args.config)
    print(args.config)
    create_temporary_databases(paths, outdir, workers, pulse_keys,args.config, start_time,db_name,gcd_rescue,verbose, max_dictionary_size, custom_truth)


# Main function call
if __name__ == '__main__':
    main()



             






