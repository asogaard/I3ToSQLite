'''

'''
from icecube import dataclasses, icetray, dataio
from icecube import genie_icetray
import os, glob
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import time
import os
from multiprocessing import Pool
import pickle

def Contains_RetroReco(frame):
    try:
        frame['L7_reconstructed_zenith']
        return True
    except:
        return False
def Build_Standard_Extraction():
    standard_truths = {'energy_log10': 'MCInIcePrimary.energy',
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

def Case_Handle_This(frame, sim_type):
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

def Build_Blank_Extraction():
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

def Build_RetroReco_Extraction():
    retro_extraction = {'azimuth_retro': 'frame["L7_reconstructed_azimuth"].value',
                        'time_retro': 'frame["L7_reconstructed_time"].value',
                        'energy_log10_retro': 'frame["L7_reconstructed_total_energy"].value', 
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
                        'energy_log10_sigma': 'frame["L7_retro_crs_prefit__energy_sigma_tot"].value',
                        'osc_weight': 'frame["I3MCWeightDict"]["weight"]',
                        'cascade_energy_retro': 'frame["L7_reconstructed_cascade_energy"].value',
                        'track_energy_retro': 'frame["L7_reconstructed_track_energy"].value',
                        'track_length_retro': 'frame["L7_reconstructed_track_length"].value',
                        'lvl7_probnu': 'frame["L7_MuonClassifier_FullSky_ProbNu"].value',
                        'lvl4_probnu': 'frame["L4_MuonClassifier_Data_ProbNu"].value',
                        'lvl7_prob_track': 'frame["L7_PIDClassifier_FullSky_ProbTrack"].value' }    
    return retro_extraction
def Extract_Retro(frame):
    contains_retro = Contains_RetroReco(frame)
    retro = {}
    if contains_retro:
        retro_extraction = Build_RetroReco_Extraction()
        for retro_variable in retro_extraction.keys():
            retro[retro_variable] = eval(retro_extraction[retro_variable]) 
    return retro

def Extract_Truth(frame, input_file, extract_these_truths = None):
    if extract_these_truths == None:
        extract_these_truths = Build_Standard_Extraction()
    mc = is_montecarlo(frame)
    sim_type = Find_Simulation_Type(mc,input_file)
    event_time =  frame['I3EventHeader'].start_time.utc_daq_time
    RunID, SubrunID, EventID, SubEventID = Extract_Event_IDs(frame)
    if mc:
        MCInIcePrimary, interaction_type, elasticity = Case_Handle_This(frame, sim_type)
        if MCInIcePrimary != None:
            ## is not noise
            truth = {}
            for truth_variable in extract_these_truths.keys():
                truth[truth_variable] = eval(extract_these_truths[truth_variable])
    else:
        ## is real data or noise   
        blank_extraction = Build_Blank_Extraction()
        truth = {}
        for truth_variable in blank_extraction.keys():
            truth[truth_variable] = eval(blank_extraction[truth_variable])
    return truth

def Extract_Features(frame, key, gcd_dict,calibration):
    charge = []
    time   = []
    width  = []
    area   = []
    rqe    = []
    x       = []
    y       = []
    z       = []
    if 'I3MCTree' in frame.keys() and key in frame.keys():
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
        
    features = {'charge_log10': charge, 
                'dom_time': time, 
                'dom_x': x, 
                'dom_y': y, 
                'dom_z': z,
                'width' : width,
                'pmt_area': area, 
                'rqe': rqe}
    return features
    

def Find_Simulation_Type(mc, input_file):
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

def Load_GeoSpatial_Data(gcd_path):
    gcd_file = dataio.I3File(gcd_path)
    g_frame = gcd_file.pop_frame(icetray.I3Frame.Geometry)
    om_geom_dict = g_frame["I3Geometry"].omgeo
    calibration = gcd_file.pop_frame(icetray.I3Frame.Calibration)["I3Calibration"]
    return om_geom_dict, calibration  

def IsEmpty(features):
    if features['dom_x'] != None:
        return False
    else:
        return True

def Extract_Event_IDs(frame):
    RunID       = frame['I3EventHeader'].run_id
    SubrunID    = frame['I3EventHeader'].sub_run_id
    EventID     = frame['I3EventHeader'].event_id
    SubEventID  = frame['I3EventHeader'].sub_event_id
    return RunID, SubrunID, EventID, SubEventID

def Apply_Event_No(extraction, event_no_list, event_counter):
    out = pd.DataFrame(extraction.values()).T
    out.columns = extraction.keys()
    out['event_no'] = event_no_list[event_counter]
    return out

def Check_For_New_Columns(columns, biggest_columns):
    if len(columns) > len(biggest_columns):
        return columns
    else:
        return biggest_columns

def WriteDicts(settings):
    input_files,id,gcd_files,outdir , max_dict_size,event_no_list, pulse_map_keys,custom_truth, db_name = settings
    # Useful bits
    event_counter = 0
    feature_big = {}
    truth_big   = pd.DataFrame()
    retro_big   = pd.DataFrame()
    file_counter = 0
    output_count = 0
    gcd_count = 0
    for u  in range(0,len(input_files)):
        input_file = input_files[u]
        print('loading gcd')
        gcd_dict, calibration = Load_GeoSpatial_Data(gcd_files[u])
        print(input_file)
        i3_file = dataio.I3File(input_file, "r")
        print('Using %s for '%gcd_files[u].split('/')[-1])
        print('for i3-file %s'%input_file.split('/')[-1])
        gcd_count  +=1
    
        while i3_file.more() :
            try:
                frame = i3_file.pop_physics()
            except:
                frame = False
            if frame :
                pulse_maps = {}
                for pulse_map_key in pulse_map_keys:
                    pulse_maps[pulse_map_key] = Extract_Features(frame,pulse_map_key, gcd_dict,calibration)
                   
                truths   = Extract_Truth(frame, input_file, custom_truth)
                truth    = Apply_Event_No(truths, event_no_list, event_counter)
                retros   = Extract_Retro(frame)
                if len(retros)>0:
                    retro   = Apply_Event_No(retros, event_no_list, event_counter)

                for pulse_map_key in pulse_map_keys:
                    is_empty = IsEmpty(pulse_maps[pulse_map_key]) 
                    if is_empty == False:
                        pulse_maps[pulse_map_key] = Apply_Event_No(pulse_maps[pulse_map_key], event_no_list, event_counter)
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
        print('WORKER %s : %s/%s'%(id,file_counter,len(input_files)))
        file_counter +=1
    if (len(feature_big) > 0):
        ### ADD STUFF HERE
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

def FindFiles(paths,gcd_rescue = None, extensions = None):
    if extensions == None:
        extensions = ("/*.i3.bz2","/*.zst","/*.gz")
    if gcd_rescue == None:
        gcd_rescue = '/groups/icecube/stuttard/data/oscNext/pass2/gcd/GeoCalibDetectorStatus_AVG_55697-57531_PASS2_SPE_withScaledNoise.i3.gz'
    input_files_mid = []
    input_files = []
    files = []
    gcd_files_mid = []
    gcd_files = []

    for path in paths:
        for extension in extensions:
            mid = glob.glob(path + extension)
            if mid != []:
                for file in mid:
                    if 'gcd' in file:
                        gcd_file = file
                    if 'GCD' in file:
                        gcd_file = file
                    if 'geo' in file:
                        gcd_file = file
                    if 'Geo' in file:
                        gcd_file = file
                    else:
                        input_files_mid.append(file)
        try:
             print(gcd_file)
        except:
            gcd_file = gcd_rescue
        for k in range(0,len(input_files_mid)):
            gcd_files_mid.append(gcd_file)
        input_files.extend(input_files_mid)
        gcd_files.extend(gcd_files_mid)
        gcd_files_mid = []
        input_files_mid = []
    return input_files, gcd_files

def CreateOutDirectory(outdir):
    try:
        os.makedirs(outdir)
        return False
    except:
        print(' !!WARNING!! \n \
            %s already exists. \n \
            ABORTING! '%outdir)
        return True

def PickleCleaner(List):
    clean_list = []
    for element in List:
        clean_list.append(str(element))
    return clean_list

def Extract_Config():
    with open('tmp/config/config.pkl', 'rb') as handle:
        config = pickle.load(handle)  
    paths = PickleCleaner(config['paths'])
    
    outdir = str(config['outdir'])
    workers = int(config['workers'])

    pulse_keys = PickleCleaner(config['pulse_keys'])
   
    db_name = str(config['db_name'])
    try:
        max_dictionary_size = int(config['max_dictionary_size'])
    except:
        max_dictionary_size = 10000
    try:
        custom_truth    =   PickleCleaner(config['custom_truth'])
        
    except:
        custom_truth    = None
    return paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth
def Transmit_Start_Time(start_time,config_path):
    with open(config_path, 'rb') as handle:
        config = pickle.load(handle)
    config['start_time'] = start_time
    with open(config_path , 'wb') as handle:
        pickle.dump(config, handle, protocol=2)  
    return
def Exit_CVMFS():
    os.system('exit')
    return
def CreateTemporaryDatabases(paths, outdir, workers, pulse_keys,config_path, start_time,db_name,max_dictionary_size = 10000, custom_truth = None):
    if __name__ == "__main__" :
        start_time = time.time()    

        input_files, gcd_files = FindFiles(paths)
        print('gcd files: %s'%len(gcd_files))
        print('i3 files: %s'%len(input_files))

        directory_exists = CreateOutDirectory(outdir + '/%s/tmp'%db_name)

        # SETTINGS
        settings = []
        event_nos = np.array_split(np.arange(0,99999999,1),workers) #Notice that this choice means event_no is NOT unique between different databases.
        file_list = np.array_split(np.array(input_files),workers)
        gcd_file_list = np.array_split(np.array(gcd_files),workers)

        for i in range(0,workers):
            settings.append([file_list[i],str(i),gcd_file_list[i],outdir,max_dictionary_size,event_nos[i], pulse_keys, custom_truth, db_name])
        #WriteDicts(settings[0])
               
        p = Pool(processes = workers)
        p.map(WriteDicts, settings)   
        p.close()
        p.join()
        Transmit_Start_Time(start_time, config_path)
        #Exit_CVMFS()
        #print('Job Complete! Time Elapsed: %s min' %((time.time() - start_time)/60))

#parser = argparse.ArgumentParser()
#parser.add_argument('-config', '--config', type = str, required = True)
#args = parser.parse_args()

    

start_time = time.time()

paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth = Extract_Config()
print(paths, outdir, workers, pulse_keys, db_name, max_dictionary_size, custom_truth)
CreateTemporaryDatabases(paths, outdir, workers, pulse_keys,'tmp/config/config.pkl', start_time,db_name, max_dictionary_size, custom_truth)




             





