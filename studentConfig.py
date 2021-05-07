import pandas as pd
import boto3
import io
import time
import datetime

#import libraries
import pandas as pd
pd.set_option('display.max_columns', None)
from pandas import Timestamp
import datetime
from datetime import datetime, timezone
import geopandas as gpd
from shapely.geometry import Point,shape,Polygon
from scipy import spatial
import numpy as np
import boto3
import io
import os
import sys
import keplergl
from keplergl import KeplerGl
import numpy as np
import matplotlib.pyplot as plt
#%matplotlib inline
import h3
import networkx as nx

# display the resolution available for h3 indexing with hexagons
def h3_indexTable():
    max_res = 15
    list_hex_edge_km = []
    list_hex_edge_m = []
    list_hex_perimeter_km = []
    list_hex_perimeter_m = []
    list_hex_area_sqkm = []
    list_hex_area_sqm = []

    for i in range(0, max_res + 1):
        ekm = h3.edge_length(resolution=i, unit='km')
        em = h3.edge_length(resolution=i, unit='m')
        list_hex_edge_km.append(round(ekm, 3))
        list_hex_edge_m.append(round(em, 3))
        list_hex_perimeter_km.append(round(6 * ekm, 3))
        list_hex_perimeter_m.append(round(6 * em, 3))

        akm = h3.hex_area(resolution=i, unit='km^2')
        am = h3.hex_area(resolution=i, unit='m^2')
        list_hex_area_sqkm.append(round(akm, 3))
        list_hex_area_sqm.append(round(am, 3))

    df_meta = pd.DataFrame({"edge_length_km": list_hex_edge_km,
                            "perimeter_km": list_hex_perimeter_km,
                            "area_sqkm": list_hex_area_sqkm,
                            "edge_length_m": list_hex_edge_m,
                            "perimeter_m": list_hex_perimeter_m,
                            "area_sqm": list_hex_area_sqm
                            })

    df_meta = df_meta[["edge_length_km", "perimeter_km", "area_sqkm", 
             "edge_length_m", "perimeter_m", "area_sqm"]]
    return(df_meta)

#perform colocation based upon hex bins
def colocate_byH3 (df,res,colo_num):
    #Get Dataframoe to work on and set ts column to utc time
    data_h3 = df.copy(deep=True)
    
    def lat_lng_to_h3(row):
        return h3.geo_to_h3(row['lat'], row['lon'], res)

    def add_geometry(row):
        points = h3.h3_to_geo_boundary(row['hexID'], True)
        return Polygon(points)

    #format ts to datetime
    #data_h3['localized_ts'] = data_h3['localized_ts'].astype(str) # done to plot ts in kepler
    #data_h3['ts'] =  pd.to_datetime(data_h3['ts'].str.strip(), errors='coerce', utc = True)
    data_h3.dropna(subset=['lat','lon','localized_ts'], inplace=True)
    data_h3['localized_ts'] = data_h3['localized_ts'].dt.strftime("%x")

    # Create a new Hex ID column describing what H3 hexagon bin each point falls within
    data_h3['hexID'] = data_h3.apply(lat_lng_to_h3 ,axis=1)

    # Create a column representing the number of unique UDIDs that appear in each hex bin
    data_h3['num_unique'] = (data_h3.groupby('hexID')['uuid'].transform('nunique'))

    # Create a column representing the list of actual unique UDIDs that appear in each hex bin, then create a joined string
    data_h3['Unique_UDID'] = data_h3.groupby('hexID')['uuid'].transform(lambda x: ', '.join(x.unique()))
    data_h3['activity_dates_of_UUIDs'] = data_h3.groupby('hexID')['localized_ts'].transform(lambda x: ', '.join(x.unique()))

    # Create a new narrowed dataset without geometry column - original one was POINT, but we will create a POLYGON one with H3
    hexDF_shape = data_h3[['hexID','uuid','num_unique','activity_dates_of_UUIDs','lat','lon']]

    #create a copy to avoid set copy warnings
    hexDF_shape = hexDF_shape.copy()

    # Create POLYGON geometry using H3's h3_to_geo_boundary, then project to WGS 1984 (which is what epsg 4326 represents)
    #
    # apply h3 to geo boundary funciton
    hexDF_shape['geometry'] = hexDF_shape.hexID.apply(lambda x: Polygon(h3.h3_to_geo_boundary(x)))

    #create geodataframe and set crs to wgs 1984
    hexDF_shape = gpd.GeoDataFrame(hexDF_shape, crs=("EPSG:4326"))

    # Filter the dataframe above for only hexagons that have more than one UDID within them
    hexDF_coloc = hexDF_shape[hexDF_shape['num_unique'] > colo_num]
    return(hexDF_coloc)

import time
import datetime

# convert supplied local time to utc time
def Local2UTC(LocalTime):
    # application:  dp6['utcNew'] = dp6['localized_ts'].apply(lambda x: Local2UTC(x))
    EpochSecond = time.mktime(LocalTime.timetuple())
    utcTime = datetime.datetime.utcfromtimestamp(EpochSecond)
    return utcTime

def clean(crse,name):
    #csv file clean-up
    filePath = "course_%s_%s_allData.csv" % (crse, name)
    #remove the file if exists
    if os.path.exists(filePath):
        os.remove(filePath)
    filePath = "course_%s_allStudent_data.csv" % crse
    #remove the file if exists
    if os.path.exists(filePath):
        os.remove(filePath)


#for use in offline mode
offline_lookup = pd.DataFrame({"name":['Aelfric','Alfred','Brida','Guthrum','Jkricket','Odda','Ragnar','Ubba'
                                              ,'Uhtred']
                               ,"udid":['801b9140-6179-4eae-b6db-fcbf5788ce0c'
                                       ,'f2957e59-a638-4e6a-92cc-9a6802c4888f'
                                       ,'bea8836d-3008-44a3-8c3c-53555f0a2dee'
                                       ,'22c07d36-5d29-4064-a3d7-f9019cd27cea'
                                       ,'ce3bd3de-8031-4b59-93f8-445a2601780c'
                                       ,'66946627-b308-4b54-a147-5f4983197b73'
                                       ,'32bdb1af-fd87-4096-a683-63cd16a264d6'
                                       ,'a266ab79-daf2-4375-a94e-5522a980e934'
                                       ,'e30234a8-a2f8-4bdd-b82e-3ac919eeec7a']})


#----------------------------------------------------------------------------------
#          Lookup for the mid by the students nobebook' name
#----------------------------------------------------------------------------------
def get_mid(dev_name,crse):
    student_uuid_list = "Student_UDID_List.csv"
    #get device_name uuid
    obj = s3.get_object(Bucket=awsBucket, Key=student_uuid_list)
    student_uuids = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0, dtype = {
        'UDID':str
        ,'Name':str
    })

    #rename columns
    student_uuids.rename(columns ={
        "UDID":"uuid"
        ,"Name":"name"
    }, inplace = True)

    #lower columns and remove any white space
    student_uuids['uuid'] = student_uuids['uuid'].str.strip().str.lower()
    #lookup the uuid by the name of the students device name
    lookup = student_uuids['name'] == '%s' % dev_name 
    #filter data
    student_uuids.where(lookup, inplace=True)
    #drop nan
    student_uuids = student_uuids.dropna()
    #get the mid for the student based upon the name of device
    maid = student_uuids.iloc[0]['uuid']
    return(maid)

#----------------------------------------------------------------------------------
#          Read all student IOT device data
#----------------------------------------------------------------------------------
def get_IoT_data(crse,start,end):
    #deviceName_Leofric
    try:
        deviceName_Leofric = "%s/leofric_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Leofric)
        student_iot_Leofric = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Leofric['name'] = 'Leofric'
    except: 
        student_iot_Leofric = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])
    #deviceName_Guthrum
    try:
        deviceName_Guthrum = "%s/guthrum_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Guthrum)
        student_iot_Guthrum = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Guthrum['name'] = 'Guthrum'
    except: 
        student_iot_Guthrum = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])
    #deviceName_Jkricket  
    try:
        deviceName_Jkricket = "%s/_jkricket_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Jkricket)
        student_iot_Jkricket = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Jkricket['name'] = 'Jkricket'
    except: 
        student_iot_Jkricket = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])
    #deviceName_Alfred
    try:
        deviceName_Alfred = "%s/alfred_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Alfred)
        student_iot_Alfred = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Alfred['name'] = 'Alfred'
    except: 
        student_iot_Alfred = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])

    #deviceName_Ragnar    
    try:
        deviceName_Ragnar = "%s/ragnar_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Ragnar)
        student_iot_Ragnar = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Ragnar['name'] = 'Ragnar'
    except: 
        student_iot_Ragnar = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])    
    #deviceName_Brida
    try:
        deviceName_Brida = "%s/brida_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Brida)
        student_iot_Brida = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Brida['name'] = 'Brida'
    except: 
        student_iot_Brida = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])   

    #deviceName_Odda
    try:
        deviceName_Odda = "%s/odda_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Odda)
        student_iot_Odda = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Odda['name'] = 'Odda'
    except: 
        student_iot_Odda = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])

    #deviceName_Athelflaed
    try:
        deviceName_Athelflaed = "%s/athelflaed_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Athelflaed)
        student_iot_Athelflaed = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Athelflaed['name'] = 'Athelflaed'
    except: 
        student_iot_Athelflaed = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type']) 
    #deviceName_Hild
    try:
        deviceName_Hild = "%s/hild_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Hild)
        student_iot_Hild = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Hild['name'] = 'Hild'
    except: 
        student_iot_Hild = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type']) 
    #deviceName_Ubba
    try:
        deviceName_Ubba = "%s/ubba_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Ubba)
        student_iot_Ubba = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Ubba['name'] = 'Ubba'
    except: 
        student_iot_Ubba = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])  
    #deviceName_Aelfric
    try:
        deviceName_Aelfric = "%s/aelfric_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Aelfric)
        student_iot_Aelfric = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Aelfric['name'] = 'Aelfric'
    except: 
        student_iot_Aelfric = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])
    #deviceName_Aelfric
    try:
        deviceName_Uhtred = "%s/uhtred_iot.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Uhtred)
        student_iot_Uhtred = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         , dtype ={'MAC':str,'FirstSeen':str,'SSID':str,'AuthMode':str,'Channel':float,'RSSI':float
                                   ,'CurrentLatitude':float,'CurrentLongitude':float,'AltitudeMeters':float,'Type':str
                                  }, low_memory=False)
        student_iot_Uhtred['name'] = 'Uhtred'
    except: 
        student_iot_Uhtred = pd.DataFrame(columns = ['name','MAC','FirstSeen','SSID','AuthMode','Channel','RSSI','CurrentLatitude'
                                                      ,'AltitudeMeters','Type'])



    # Concat dataframes into one for combined data set from source
    student_iot = pd.concat([student_iot_Leofric, student_iot_Guthrum, student_iot_Jkricket,student_iot_Alfred,student_iot_Ragnar
                   ,student_iot_Brida,student_iot_Odda,student_iot_Athelflaed,student_iot_Hild,student_iot_Ubba
                   ,student_iot_Aelfric,student_iot_Uhtred])

    #reset the index due to concating multiple dataframes together
    student_iot = student_iot.reset_index()

    #rename columns to lowercase standard
    student_iot.rename(columns = {
        "MAC" : "mac"
        ,"SSID":"ssid"
        ,"AuthMode":"authMode"
        ,"FirstSeen":"localized_ts"
        ,"Channel":"channel"
        ,"RSSI":"rssi"
        ,"CurrentLatitude":"lat"
        ,"CurrentLongitude":"lon"
        ,"AltitudeMeters":"altitude"
        ,"AccuracyMeters":"accuracy"
        ,"Type":"type"
    }, inplace = True)

    #Add Source Column
    student_iot['source'] = 'iot'

    # lower case columns and strip out any whitespace that may be present
    student_iot['mac'] = student_iot['mac'].str.strip().str.lower()
    student_iot['ssid'] = student_iot['ssid'].str.strip().str.lower()
    student_iot['authMode'] = student_iot['authMode'].str.strip().str.lower()
    student_iot['type'] = student_iot['type'].str.strip().str.lower()

    #covert to_datetime
    student_iot['tzID'] = 'US/Eastern'
    # --------- original
    #student_iot['localized_ts'] =  pd.to_datetime(student_iot['localized_ts'].str.strip(), errors='coerce', utc = True)
    #
    #localize the timezone id
    #student_iot['localized_ts'] = student_iot['localized_ts'].dt.tz_convert('US/Eastern')
    #
    #---- New code
    #
    student_iot['localized_ts'] = pd.to_datetime(student_iot['localized_ts'].str.strip(), errors ='coerce',utc = True)
    #enforce timezone as us/eastern
    student_iot['localized_ts'] = student_iot['localized_ts'].dt.tz_convert('US/Eastern')+pd.Timedelta(hours=4)
    # --- end new code
    student_iot['dtg'] = pd.to_datetime(student_iot['localized_ts'])
    student_iot['dtg'] = student_iot['dtg'].dt.date
    student_iot['dtg'] = pd.to_datetime(student_iot['dtg'])
    #make sure we don't have any data violating epoch time
    mask = (student_iot['dtg'] >= start) & (student_iot['dtg'] <=end)
    student_iot = student_iot.loc[mask]
    #convert local time to utc time
    student_iot['utc_ts'] = student_iot['localized_ts'].apply(lambda x: Local2UTC(x))
    #add in unix_timestamp
    student_iot['unix_timestamp'] = student_iot['utc_ts'].apply(lambda x: x.timestamp())
    student_iot['unix_timestamp'] = student_iot['unix_timestamp'].astype(int)

    # ----------- read name to device master file --------------------
    student_uuid_list = "Student_UDID_List.csv"
    #get device_name uuid
    obj = s3.get_object(Bucket=awsBucket, Key=student_uuid_list)
    student_uuids = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0, dtype = {
        'UDID':str
        ,'Name':str
    })

    #rename columns
    student_uuids.rename(columns ={
        "UDID":"uuid"
        ,"Name":"name"
    }, inplace = True)

    #lower columns and remove any white space
    student_uuids['uuid'] = student_uuids['uuid'].str.strip().str.lower()

    # ----------------------- Join the two dataframes together -----------------------
    student_iot = pd.merge(student_iot,student_uuids,on='name')

    #reorder columns
    student_iot = student_iot[['dtg','name','uuid','source','lat','lon','unix_timestamp','utc_ts','localized_ts','tzID','type','ssid'
                               ,'mac','altitude','accuracy','rssi','channel','authMode']]
    
    # deal with nan
    student_iot = student_iot.fillna('')
    
    #return the results
    return(student_iot)

#----------------------------------------------------------------------------------
#          Read all student App device data
#----------------------------------------------------------------------------------
def get_app_data(crse,start,end):
    #deviceName_Leofric
    try:
        deviceName_Leofric = "%s/Leofric_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Leofric)
        student_app_Leofric = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Leofric['name'] = 'Leofric'
    except: 
        student_app_Leofric = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                      ,'Speed','Address'])
    #deviceName_Guthrum
    try:
        deviceName_Guthrum = "%s/Guthrum_App.csv" % crse
        obj = s3.get_object(Bucket=bucket, Key=deviceName_Guthrum)
        student_app_Guthrum = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Guthrum['name'] = 'Guthrum'
    except: 
        student_app_Guthrum = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                      ,'Speed','Address'])
    #deviceName_Jkricket  
    try:
        deviceName_Jkricket = "%s/_Jkricket_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Jkricket)
        student_app_Jkricket = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Jkricket['name'] = 'Jkricket'
    except: 
        student_app_Jkricket = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                      ,'Speed','Address'])        
        
    #deviceName_Alfred
    try:
        deviceName_Alfred = "%s/Alfred_App.csv" % (crse)       
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Alfred)
        student_app_Alfred = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Alfred['name'] = 'Alfred'
    except: 
        student_app_Alfred = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                      ,'Speed','Address'])               
    #deviceName_Ragnar    
    try:
        deviceName_Ragnar = "%s/Ragnar_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Ragnar)       
        student_app_Ragnar = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Ragnar['name'] = 'Ragnar'
    except: 
        student_app_Ragnar = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])      
   #deviceName_Brida
    try:
        deviceName_Brida = "%s/Brida_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Brida) 
        student_app_Brida = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Brida['name'] = 'Brida'
    except: 
        student_app_Brida = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])   
        
    #deviceName_Odda
    try:
        deviceName_Odda = "%s/Odda_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Odda) 
        student_app_Odda = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Odda['name'] = 'Odda'
    except: 
        student_app_Odda = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address']) 
        
    #deviceName_Athelflaed
    try:
        deviceName_Athelflaed = "%s/Athelflaed_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Athelflaed)       
        student_app_Athelflaed = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Athelflaed['name'] = 'Athelflaed'
    except: 
        student_app_Athelflaed = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])        
    #deviceName_Hild
    try:
        deviceName_Hild = "%s/Hild_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Hild)        
        student_app_Hild = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float,'Altitude':str
                                  ,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Hild['name'] = 'Hild'
    except: 
        student_app_Hild = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])             
    #deviceName_Ubba
    try:
        deviceName_Ubba = "%s/Ubba_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Ubba)
        student_app_Ubba = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float
                                                  ,'Altitude':str,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Ubba['name'] = 'Ubba'
    except: 
        student_app_Ubba = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address']) 
    #deviceName_Aelfric
    try:
        deviceName_Aelfric = "%s/Aelfric_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Aelfric)        
        student_app_Aelfric = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float
                                                  ,'Altitude':str,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Aelfric['name'] = 'Aelfric'
    except: 
        student_app_Aelfric = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])
       #deviceName_Uhtred
    try:
        deviceName_Uhtred = "%s/Uhtred_App.csv" % (crse)
        obj = s3.get_object(Bucket=awsBucket, Key=deviceName_Uhtred)        
        student_app_Uhtred = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0
                                         ,dtype ={'Valid':str,'Time':str,'Latitude':float,'Longitude':float
                                                  ,'Altitude':str,'Accuracy':str,'Speed':str,'Address':str}, low_memory=False)
        student_app_Uhtred['name'] = 'Uhtred'
    except: 
        student_app_Uhtred = pd.DataFrame(columns = ['name','Valid','Time','Latitude','Longitude','Altitude','Accuracy'
                                                   ,'Speed','Address'])
        
    # Concat dataframes into one for combined data set from source
    student_app = pd.concat([student_app_Leofric, student_app_Guthrum,student_app_Jkricket,student_app_Ragnar
                            ,student_app_Brida,student_app_Odda,student_app_Athelflaed,student_app_Hild
                            ,student_app_Ubba,student_app_Aelfric,student_app_Uhtred])
 
    #Add Source Column
    student_app['source'] = 'app'   
    #format the dataframe
    student_app['Altitude'] = student_app['Altitude'].replace(' m','', regex=True).astype(float)
    student_app['Accuracy'] = student_app['Accuracy'].replace(' m','', regex=True).astype(float)
    student_app['Speed'] = student_app['Speed'].replace(' mph','', regex=True).astype(float)
    student_app['Address'] = student_app['Address'].replace('°','', regex=True).astype(str)
 
    #rename columns to lowercase standard
    student_app.rename(columns = {
        "Valid":"valid"
        ,"Time":"localized_ts"
        ,"Latitude":"lat"
        ,"Longitude":"lon"
        ,"Altitude":"altitude"
        ,"Accuracy":"accuracy"
        ,"Speed":"speed"
        ,"Address":"latLon_string"
    }, inplace = True)
    # lower case columns and strip out any whitespace that may be present
    student_app['valid'] = student_app['valid'].str.strip().str.lower()
    student_app['latLon_string'] = student_app['latLon_string'].str.strip().str.lower()

    #covert to_datetime
    student_app['tzID'] = 'US/Eastern'
    #
    # -------- original code
    #student_app['localized_ts'] =  pd.to_datetime(student_app['localized_ts'].str.strip(), errors='coerce', utc = True)
    #localize the timezone id
    #student_app['localized_ts'] = student_app['localized_ts'].dt.tz_convert('US/Eastern')
    #
    # ---------- end original code
    #
    # ----------- new code
    # convert to timezone aware dataframe
    student_app['localized_ts'] = pd.to_datetime(student_app['localized_ts'].str.strip(), errors ='coerce',utc = True)
    #enforce timezone as us/eastern
    student_app['localized_ts'] = student_app['localized_ts'].dt.tz_convert('US/Eastern')+pd.Timedelta(hours=5)
    #
    # ----- end new code
    student_app['dtg'] = pd.to_datetime(student_app['localized_ts'])
    student_app['dtg'] = student_app['dtg'].dt.date
    student_app['dtg'] = pd.to_datetime(student_app['dtg'])
    #make sure we don't have any data violating epoch time
    mask = (student_app['dtg'] >= start) & (student_app['dtg'] <=end)
    student_app = student_app.loc[mask]
    #convert local time to utc time
    student_app['utc_ts'] = student_app['localized_ts'].apply(lambda x: Local2UTC(x))
    #add in unix_timestamp
    student_app['unix_timestamp'] = student_app['utc_ts'].apply(lambda x: x.timestamp())
    student_app['unix_timestamp'] = student_app['unix_timestamp'].astype(int)
    #
    # ----------- read name to device master file --------------------
    student_uuid_list = "Student_UDID_List.csv"
    #get device_name uuid
    obj = s3.get_object(Bucket=awsBucket, Key=student_uuid_list)
    student_uuids = pd.read_csv(io.BytesIO(obj['Body'].read()),header=0, dtype = {
        'UDID':str
        ,'Name':str
    })

    #rename columns
    student_uuids.rename(columns ={
        "UDID":"uuid"
        ,"Name":"name"
    }, inplace = True)

    #lower columns and remove any white space
    student_uuids['uuid'] = student_uuids['uuid'].str.strip().str.lower()

    # ----------------------- Join the two dataframes together -----------------------
    student_app = pd.merge(student_app,student_uuids,on='name')
    #reorder columns
    student_app = student_app[['dtg','name','uuid','source','lat','lon','unix_timestamp','utc_ts','localized_ts','tzID','altitude'
                               ,'accuracy','speed','latLon_string','valid']]
    
    # deal with nan
    student_app = student_app.fillna('')
    #return the dataframe as results of union of all app student data with uuid join by name
    return(student_app)

#Merge the data together
def mergeAll_data(student_appAll,student_iotAll):
    dataAll = pd.concat([student_appAll,student_iotAll])
    #reset index from concat function
    dataAll.reset_index(inplace = True, drop = True)

    # deal with the nan
    dataAll[['lat','lon','accuracy','gps_speed','altitude','speed']] = dataAll[['lat','lon','accuracy','gps_speed'
                                                                                ,'altitude','speed']].fillna(value=0)
    dataAll = dataAll.fillna('')

    #format date columns
    dataAll['utc_ts'] = dataAll['utc_ts'].astype(str)
    dataAll['utc_ts'] =  pd.to_datetime(dataAll['utc_ts'].str.strip(), errors='coerce', utc = True)

    #formate datatypes
    dataAll['lat'] = dataAll['lat'].astype(float)
    dataAll['lon'] = dataAll['lon'].astype(float)
    dataAll['altitude'] = dataAll['altitude'].astype(float)
    dataAll['speed'] = dataAll['speed'].astype(float)

    #add a unique ID to dataframe
    dataAll.insert(0, 'id', range(0, 0 + len(dataAll)))
    dataAll = dataAll.fillna('')
    
    #return dataframe
    return(dataAll)

# kepler config
config1 = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "euwj4t8",
          "type": "point",
          "config": {
            "dataId": "all_data",
            "label": "Point",
            "color": [
              18,
              147,
              154
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "ColorBrewer Paired-12",
                "type": "qualitative",
                "category": "ColorBrewer",
                "colors": [
                  "#a6cee3",
                  "#1f78b4",
                  "#b2df8a",
                  "#33a02c",
                  "#fb9a99",
                  "#e31a1c",
                  "#fdbf6f",
                  "#ff7f00",
                  "#cab2d6",
                  "#6a3d9a",
                  "#ffff99",
                  "#b15928"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": {
              "name": "name",
              "type": "string"
            },
            "colorScale": "ordinal",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "all_data": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "app",
                "format": None
              },
              {
                "name": "user_agent",
                "format": None
              },
              {
                "name": "ipv_4",
                "format": None
              },
              {
                "name": "carrier",
                "format": None
              },
              {
                "name": "keywords",
                "format": None
              },
              {
                "name": "altitude",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 0.5,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}
config2 = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "mdhffdp",
          "type": "point",
          "config": {
            "dataId": "time_filtered_data",
            "label": "Point",
            "color": [
              221,
              178,
              124
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Ice And Fire",
                "type": "diverging",
                "category": "Uber",
                "colors": [
                  "#0198BD",
                  "#49E3CE",
                  "#E8FEB5",
                  "#FEEDB1",
                  "#FEAD54",
                  "#D50255"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": {
              "name": "source",
              "type": "string"
            },
            "colorScale": "ordinal",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "time_filtered_data": [
              {
                "name": "id",
                "format": None
              },
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              },
              {
                "name": "ipv_4",
                "format": None
              },
              {
                "name": "carrier",
                "format": None
              },
              {
                "name": "user_agent",
                "format": None
              },
              {
                "name": "ssid",
                "format": None
              },
              {
                "name": "keywords",
                "format": None
              },
              {
                "name": "app",
                "format": None
              },
              {
                "name": "place_name",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 0.5,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}
config3 = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "0ccvbn",
          "type": "point",
          "config": {
            "dataId": "two_devices",
            "label": "Point",
            "color": [
              130,
              154,
              227
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Ice And Fire",
                "type": "diverging",
                "category": "Uber",
                "colors": [
                  "#0198BD",
                  "#49E3CE",
                  "#E8FEB5",
                  "#FEEDB1",
                  "#FEAD54",
                  "#D50255"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": {
              "name": "name",
              "type": "string"
            },
            "colorScale": "ordinal",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "two_devices": [
              {
                "name": "dtg",
                "format": None
              },
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 0.5,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}
config_yourData = {
  "version": "v1",
  "config": {
    "visState": {
      "filters": [],
      "layers": [
        {
          "id": "o12ushs",
          "type": "point",
          "config": {
            "dataId": "rtb_data",
            "label": "rtb_data",
            "color": [
              227,
              26,
              26
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": []
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        },
        {
          "id": "83sy8io",
          "type": "point",
          "config": {
            "dataId": "sdk_data",
            "label": "sdk_data",
            "color": [
              214,
              82,
              0
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        },
        {
          "id": "jyp056",
          "type": "point",
          "config": {
            "dataId": "yourdp6SDK_source",
            "label": "dp6_sdk",
            "color": [
              207,
              23,
              80
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        },
        {
          "id": "yl7d618p",
          "type": "point",
          "config": {
            "dataId": "app_data",
            "label": "app_data",
            "color": [
              23,
              184,
              190
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        },
        {
          "id": "7oclpdu",
          "type": "point",
          "config": {
            "dataId": "IOT_data",
            "label": "iot_data",
            "color": [
              254,
              242,
              26
            ],
            "columns": {
              "lat": "lat",
              "lng": "lon",
              "altitude": None
            },
            "isVisible": True,
            "visConfig": {
              "radius": 10,
              "fixedRadius": False,
              "opacity": 0.8,
              "outline": False,
              "thickness": 2,
              "strokeColor": None,
              "colorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "strokeColorRange": {
                "name": "Global Warming",
                "type": "sequential",
                "category": "Uber",
                "colors": [
                  "#5A1846",
                  "#900C3F",
                  "#C70039",
                  "#E3611C",
                  "#F1920E",
                  "#FFC300"
                ]
              },
              "radiusRange": [
                0,
                50
              ],
              "filled": True
            },
            "hidden": False,
            "textLabel": [
              {
                "field": None,
                "color": [
                  255,
                  255,
                  255
                ],
                "size": 18,
                "offset": [
                  0,
                  0
                ],
                "anchor": "start",
                "alignment": "center"
              }
            ]
          },
          "visualChannels": {
            "colorField": None,
            "colorScale": "quantile",
            "strokeColorField": None,
            "strokeColorScale": "quantile",
            "sizeField": None,
            "sizeScale": "linear"
          }
        }
      ],
      "interactionConfig": {
        "tooltip": {
          "fieldsToShow": {
            "rtb_data": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              }
            ],
            "sdk_data": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              }
            ],
            "yourdp6SDK_source": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              }
            ],
            "app_data": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              }
            ],
            "IOT_data": [
              {
                "name": "name",
                "format": None
              },
              {
                "name": "uuid",
                "format": None
              },
              {
                "name": "source",
                "format": None
              },
              {
                "name": "lat",
                "format": None
              },
              {
                "name": "lon",
                "format": None
              },
              {
                "name": "localized_ts",
                "format": None
              }
            ]
          },
          "compareMode": False,
          "compareType": "absolute",
          "enabled": True
        },
        "brush": {
          "size": 0.5,
          "enabled": False
        },
        "geocoder": {
          "enabled": False
        },
        "coordinate": {
          "enabled": False
        }
      },
      "layerBlending": "normal",
      "splitMaps": [],
      "animationConfig": {
        "currentTime": None,
        "speed": 1
      }
    },
    "mapState": {
      "bearing": 0,
      "dragRotate": False,
      "latitude": 38.89272523796748,
      "longitude": -77.22862603954887,
      "pitch": 0,
      "zoom": 12.021321720342172,
      "isSplit": False
    },
    "mapStyle": {
      "styleType": "dark",
      "topLayerGroups": {},
      "visibleLayerGroups": {
        "label": True,
        "road": True,
        "border": False,
        "building": True,
        "water": True,
        "land": True,
        "3d building": False
      },
      "threeDBuildingColor": [
        9.665468314072013,
        17.18305478057247,
        31.1442867897876
      ],
      "mapStyles": {}
    }
  }
}

print('Module Import Completed')