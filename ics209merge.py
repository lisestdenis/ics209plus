import pandas as pd
import numpy as np
import datetime as dt
import ics209util
import earthpy as et
import os
import ast

lgcy_timespan = '1999to2002'
hist_timespan = '2001to2013'
curr_timespan = '2014to2020'
final_timespan = '1999to2020'
allhist_timespan = '1999to2013'

data_dir = os.path.join(et.io.HOME, 'data')
out_dir = os.path.join(data_dir, 'out')
tmp_dir = os.path.join(data_dir,'tmp')
fod_dir = os.path.join(data_dir,'raw','excel','fod')
cpx_dir = os.path.join(data_dir,'raw','cpx_assocs')
fired_dir = os.path.join(data_dir,'raw','inc_fired')
latlon_dir = os.path.join(data_dir,'raw','latlong_clean')

fod_version = '20220819'
cpx_version= '20190815'

def _historical1_rename_columns(df_h1):
    # rename columns
    df_h1.columns = df_h1.columns.str.replace('REPDATE','REPORT_TO_DATE')
    df_h1.columns = df_h1.columns.str.replace('EVENT_ID','INCIDENT_NUMBER')
    df_h1.columns = df_h1.columns.str.replace('LOCATE','POO_SHORT_LOCATION_DESC')
    df_h1.columns = df_h1.columns.str.replace('ECOSTS','EST_IM_COST_TO_DATE')
    df_h1.columns = df_h1.columns.str.replace('F_CONTAIN','PCT_CONTAINED_COMPLETED')
    df_h1.columns = df_h1.columns.str.replace('CDATE','EXPECTED_CONTAINMENT_DATE')
    df_h1.columns = df_h1.columns.str.replace('TEAMTYPE','INC_MGMT_ORG_ABBREV')
    df_h1.columns = df_h1.columns.str.replace('TEAMNAME','INCIDENT_COMMANDERS_NARR')
    df_h1.columns = df_h1.columns.str.replace('DPRIORITY','DISPATCH_PRIORITY')
    df_h1.columns = df_h1.columns.str.replace('GPRIORITY','GACC_PRIORITY')
    df_h1.columns = df_h1.columns.str.replace('STARTDATE','DISCOVERY_DATE')
    df_h1.columns = df_h1.columns.str.replace('NARRATIVE','REMARKS')
    df_h1.columns = df_h1.columns.str.replace('UN_USTATE','POO_STATE')
    df_h1.columns = df_h1.columns.str.replace('PERSONNEL','TOTAL_PERSONNEL')
    df_h1.columns = df_h1.columns.str.replace('ENAME','INCIDENT_NAME')
    df_h1.columns = df_h1.columns.str.replace('UN_UNITID','UNIT_OR_OTHER_NARR')
    df_h1['FIRE_EVENT_ID'] = df_h1.INCIDENT_NUMBER
    
    return df_h1

def _historical2_rename_columns(df_h2):
    # rename columns
    df_h2.columns = df_h2.columns.str.replace('AREA','CURR_INCIDENT_AREA') #problematic
    df_h2.columns = df_h2.columns.str.replace('CURR_INCIDENT_AREA_MEASUREMENT','CURR_INC_AREA_UOM') #fix1 AREA_MEASUREMENT
    df_h2.columns = df_h2.columns.str.replace('EST_FINAL_CURR_INCIDENT_AREA','PROJ_INCIDENT_AREA') # fix2 EST_FINAL_AREA
    df_h2.columns = df_h2.columns.str.replace('COOP_AGENCIES','ADDTNL_COOP_ASSIST_ORG_NARR')
    df_h2.columns = df_h2.columns.str.replace('COSTS_TO_DATE','EST_IM_COST_TO_DATE')
    df_h2.columns = df_h2.columns.str.replace('COUNTY','POO_COUNTY')
    df_h2.columns = df_h2.columns.str.replace('START_DATE','DISCOVERY_DATE') # order matters, must preceed next row
    df_h2.columns = df_h2.columns.str.replace('DEMOBE_START','PROJ_SIG_RES_DEMOB_START_DATE')
    df_h2.columns = df_h2.columns.str.replace('EST_FINAL_AREA_NUM','PROJ_INCIDENT_AREA')
    df_h2.columns = df_h2.columns.str.replace('EST_FINAL_COSTS','PROJECTED_FINAL_IM_COST')
    df_h2.columns = df_h2.columns.str.replace('EXP_CONTAIN','EXPECTED_CONTAINMENT_DATE')
    df_h2.columns = df_h2.columns.str.replace('FUELS','HAZARDS_MATLS_INVOLVMENT_NARR')
    df_h2.columns = df_h2.columns.str.replace('IC_NAME','INCIDENT_COMMANDERS_NARR')
    df_h2.columns = df_h2.columns.str.replace('IMT_TYPE_DESC','INC_MGMT_ORG_DESC')
    df_h2.columns = df_h2.columns.str.replace('IMT_TYPE','INC_MGMT_ORG_ABBREV')
    df_h2.columns = df_h2.columns.str.replace('LATITUDE','POO_LATITUDE')
    df_h2.columns = df_h2.columns.str.replace('LONGITUDE','POO_LONGITUDE')
    df_h2.columns = df_h2.columns.str.replace('LOCATION','POO_SHORT_LOCATION_DESC')
    df_h2.columns = df_h2.columns.str.replace('P_CONTAIN','PCT_CONTAINED_COMPLETED')
    df_h2.columns = df_h2.columns.str.replace('SIG_EVENT','SIGNIF_EVENTS_SUMMARY')
    df_h2.columns = df_h2.columns.str.replace('UN_USTATE','POO_STATE')
    df_h2.columns = df_h2.columns.str.replace('IC_NAME','INCIDENT_COMMANDERS_NARR')
    df_h2.columns = df_h2.columns.str.replace('UN_UNITID','UNIT_OR_OTHER_NARR')
    
    return df_h2

def _final_alignments(df):
    # lookup codes:
    lu_tbl = pd.read_csv(os.path.join(out_dir,'SIT209_LOOKUP_CODES.csv'))
    
    #incident type
    df['INCTYP_IDENTIFIER'] = df.INCTYP_IDENTIFIER.astype(int) # reads in as float for some reason
    inctyp_rows = lu_tbl[lu_tbl.CODE_TYPE == 'INCIDENT_TYPE']
    inc_lu = inctyp_rows[['LUCODES_IDENTIFIER','CODE_NAME','ABBREVIATION']]
    inc_lu.columns = ['INCTYP_IDENTIFIER','INCTYP_DESC','INCTYP_ABBREVIATION']
    df = df.merge(inc_lu, how='left')
    
    # set the legacy inc types
    df.loc[df.INCTYP_IDENTIFIER == 1,'INCTYP_DESC'] = 'Wildland Fire Used for Resource Benefit'
    df.loc[df.INCTYP_IDENTIFIER == 1,'INCTYP_ABBREVIATION'] = 'WFU'
    df.loc[df.INCTYP_IDENTIFIER == 2,'INCTYP_DESC'] = 'Prescribed Fire'
    df.loc[df.INCTYP_IDENTIFIER == 2,'INCTYP_ABBREVIATION'] = 'RX'
    
    # date formatting and DOY variables
    df['DISCOVERY_DATE'] = pd.to_datetime(df.DISCOVERY_DATE, errors='coerce')
    df['DISCOVERY_DOY'] = df.DISCOVERY_DATE.dt.dayofyear
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE, errors='coerce')
    df['REPORT_DOY'] = df.REPORT_TO_DATE.dt.dayofyear
    df['DISCOVERY_DATE_CORRECTED'] = pd.to_datetime(df.DISCOVERY_DATE_CORRECTED, errors='coerce')
     
    df.loc[(df.DISCOVERY_DATE_CORRECTED.notnull()) & (df.INCTYP_ABBREVIATION.isin(['WF','WFU'])) & \
           (df.DISCOVERY_DATE != df.DISCOVERY_DATE_CORRECTED),'DISCOVERY_DATE'] = df.DISCOVERY_DATE_CORRECTED
    
    df.loc[~df.POO_COUNTY.isnull(),'POO_COUNTY'] = df.POO_COUNTY.astype(str).str.title()
    df.loc[~df.POO_CITY.isnull(),'POO_CITY'] = df.POO_CITY.astype(str).str.title()
    
    return df

def _drop_extra_columns(df):
    df = df.drop(['ACTIVE',
                  'ADDNTL_FUEL_MODEL_IDENTIFIER',
                  'APPROVED_BY',
                  'APPROVED_DATE',
                  'AREA_MEASUREMENT',
                  'C_RH',
                  'C_TEMP',
                  'C_WIND_SPEED',
                  'C_WIND_DIRECTION',
                  'CAUSE_IDENTIFIER',
                  'COMMUNITIES_THREATENED_12',
                  'COMMUNITIES_THREATENED_24',
                  'COMMUNITIES_THREATENED_48',
                  'COMMUNITIES_THREATENED_72',
                  'COMPLEXITY_LEVEL_IDENTIFIER',
                  'CONTROLLED_DATE','CREATED_BY',
                  'CREATED_DATE',
                  'CRITICAL_RES',
                  'CRITICAL_RES24',
                  'CRITICAL_RES48',
                  'CRITICAL_RES72',
                  'CRIT_RES_NEEDS_12',
                  'CRIT_RES_NEEDS_24',
                  'CRIT_RES_NEEDS_48',
                  'CRIT_RES_NEEDS_72',
                  'CRIT_RES_NEEDS_GT72',
                  'CURR_INC_AREA_UOM_IDENTIFIER',
                  'CURRENT_THREAT_12',
                  'CURRENT_THREAT_24',
                  'CURRENT_THREAT_48',
                  'CURRENT_THREAT_72',
                  'CURRENT_THREAT_GT72',
                  'DATA_ENTRY_STATUS',
                  'DISCOVERY_DATE_CORRECTED', 
                  'DONWCGU_PROT_UNIT_IDENTIFIER',
                  'DOU_IDENTIFIER',
                  'END_YEAR',
                  'EST_CONTROL',
                  'F_RH',
                  'F_TEMP',
                  'F_WIND_SPEED',
                  'F_WIND_DIRECTION',
                  'FIRE_BEHAVIOR_1_IDENTIFIER',
                  'FIRE_BEHAVIOR_2_IDENTIFIER',
                  'FIRE_BEHAVIOR_3_IDENTIFIER',
                  'FIRE_INCIDENT_NUMBER',
                  'FIRECODE',
                  'FOD_LATITUDE',
                  'FOD_LONGITUDE',
                  'FUEL_MODEL_IDENTIFIER',
                  'GACC_OBS_FIRE_BEHAVE',
                  'GACC_PLANNED_ACTIONS',
                  'GACC_REMARKS',
                  'GACC_SIGNIF_EVENTS_SUMMARY',
                  'GEN_FIRE_BEHAVIOR_IDENTIFIER',
                  'HOUR',
                  'ID',
                  'INC_IDENTIFIER_OLD',
                  'INC_MGMT_ORG_IDENTIFIER',
                  'INC_MGMT_ORG_ABBREV',
                  'INCIDENT_NAME_CORRECTED',
                  'INCIDENT_NUMBER_CORRECTED',
                  'INCTYP_IDENTIFIER',
                  'ITYPE',
                  'LAST_MODIFIED_BY',
                  'LAST_MODIFIED_DATE',
                  'lat_c',
                  'LATDEG',
                  'LATMIN',
                  'LAST_EDIT',
                  'LINE_MEASUREMENT',
                  'LONGDEG',
                  'LONGMIN',
                  'LINE_TO_BUILD',
                  'LINE_TO_BUILD_NUM',
                  'LOCAL_TIMEZONE_IDENTIFIER',
                  'long_c',
                  'MANAGEMENT_CODE',
                  'NEWACRES',
                  'NO_EVACUATION',
                  'NO_LIKELY',
                  'OWNERSHIP_STATE',
                  'OWNERSHIP_STATE',
                  'OWNERSHIP_UNITID',
                  'PCT_CONT_COMPL_UOM_IDENTIFIER',
                  'PERCENT_MMA',
                  'POO_COUNTY_CODE',
                  'POO_DONWCGU_OWN_IDENTIFIER',
                  'POO_LD_PM_IDENTIFIER',
                  'POO_LD_QTR_QTR_QTR_QTR_SEC',
                  'POO_LD_QTR_QTR_QTR_SEC',
                  'POO_STATE_CODE',
                  'PREPARED_BY',
                  'PREPARED_DATE',
                  'PRIMARY_FUEL_MODEL',
                  'PROJ_INC_AREA_UOM_IDENTIFIER',
                  'PROJECTED_ACTIVITY_12',
                  'PROJECTED_ACTIVITY_24',
                  'PROJECTED_ACTIVITY_48',
                  'PROJECTED_ACTIVITY_72',
                  'PROJECTED_ACTIVITY_GT72',
                  'PROJECTED_MOVEMENT',
                  'PROJECTED_MOVEMENT24',
                  'PROJECTED_MOVEMENT48',
                  'PROJECTED_MOVEMENT72',
                  'REPORT_DATE',
                  'REPORT_NUMBER',
                  'RES_THREAT',
                  'SECNDRY_FUEL_MODEL_IDENTIFIER',
                  'SENT_DATE',
                  'SENT_TO',
                  'SENT_FROM',
                  'SEQ_NUM',
                  'SINGLE_COMPLEX_FLAG',
                  'STRATEGIC_DISCUSSION',
                  'STRATEGIC_OBJECTIVES',
                  'SUBMITTED_DATE',
                  'SUBMITTED_TO',
                  'TYPE_INC',
                  'WFIP_STAGE'
                 ],axis=1)
 
    return df

def _general_field_cleaning(df):
    
    # fix blank discovery dates
    df.loc[df.INCIDENT_ID == '2000_ID-CWF-18505_FROG LAKE','DISCOVERY_DATE'] = pd.Timestamp('2000-08-11 00:00:00')
    df.loc[df.INCIDENT_ID == '2002_CA-FKU-014204_DINKEY MTN. VMP','DISCOVERY_DATE'] = pd.Timestamp('2002-11-04 17:30:00')
    df.loc[df.INCIDENT_ID == '2002_CA-SLU-005680_PHOENIX CANYON VMP','DISCOVERY_DATE'] = pd.Timestamp('2002-11-19 16:33:00')
    df.loc[df.INCIDENT_ID == '2003_CA-FKU-000264_VAN BOXTEL VMP','DISCOVERY_DATE'] = pd.Timestamp('2003-09-26 16:00:00')
    df.loc[df.INCIDENT_ID == '2003_CA-SLU-004093_SILVA VMP','DISCOVERY_DATE'] = pd.Timestamp('2003-10-03 15:00:00')
    df.loc[df.INCIDENT_ID == '2004_CA-SLU-008636_PORTER #4 VMP','DISCOVERY_DATE'] = pd.Timestamp('2003-10-03 15:00:00')
    df.loc[df.INCIDENT_ID == '2005_CA-RRU-41175_JOHNSON RANCH VMP','DISCOVERY_DATE'] = pd.Timestamp('2005-05-31 14:56:00')
    df.loc[df.INCIDENT_ID == '2006_CA-RRU-001596_RED HILL VMP','DISCOVERY_DATE'] = pd.Timestamp('2006-01-09 08:00:00')

def _event_smoothing_prep(df):
    df['EVENT_ID'] = df['FIRE_EVENT_ID']
    print(df.EVENT_ID.isnull().sum())
    df.loc[df.EVENT_ID.isnull(),'EVENT_ID'] =  df.INCIDENT_NUMBER.astype(str).str.strip() + "|" + \
                                               df.START_YEAR.astype(str) + "|1"
    print(df.EVENT_ID.isnull().sum())
    df = df.sort_values(['EVENT_ID','REPORT_TO_DATE'])
    df = df.reset_index(drop=True)
    
    df.loc[(df.POO_LATITUDE == 0) | (df.POO_LATITUDE.isnull()) | (df.POO_LONGITUDE == 0) | (df.POO_LONGITUDE.isnull()),\
           'POO_LATITUDE'] = np.nan
    df.loc[(df.POO_LATITUDE == 0) | (df.POO_LATITUDE.isnull()) | (df.POO_LONGITUDE == 0) | (df.POO_LONGITUDE.isnull()),\
           'POO_LONGITUDE'] = np.nan
    df.loc[df.POO_LONGITUDE > 0,'POO_LONGITUDE'] = df.POO_LONGITUDE * -1
    df.loc[df.POO_LATITUDE < 0,'POO_LATITUDE'] = df.POO_LATITUDE * -1
    
    df.EST_IM_COST_TO_DATE.replace(0, np.nan,inplace=True)
    df.PROJECTED_FINAL_IM_COST.replace(0,np.nan,inplace=True)
    df.ACRES.replace(0, np.nan,inplace=True)
    df.STR_DAMAGED.replace(0, np.nan,inplace=True)
    df.STR_DESTROYED.replace(0, np.nan,inplace=True)
    df.STR_DAMAGED_COMM.replace(0, np.nan,inplace=True)
    df.STR_DESTROYED_COMM.replace(0, np.nan,inplace=True)
    df.STR_DAMAGED_RES.replace(0, np.nan,inplace=True)
    df.STR_DESTROYED_RES.replace(0, np.nan,inplace=True)
    df.POO_LATITUDE.replace(0, np.nan,inplace=True)
    df.POO_LONGITUDE.replace(0, np.nan,inplace=True)
    rows = df.shape[0]
    total_null_ctd = df.EST_IM_COST_TO_DATE.isnull().sum()
    print("null: {} percent: {}".format(df.EST_IM_COST_TO_DATE.isnull().sum(),(total_null_ctd/rows)))
    total_null_proj = df.PROJECTED_FINAL_IM_COST.isnull().sum()
    print("null: {} percent: {}".format(df.PROJECTED_FINAL_IM_COST.isnull().sum(),(total_null_proj/rows)))
    
    return df

def _cost_adjustments(df):
    
    df.loc[(df.INC_IDENTIFIER == 634117) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-07-18 16:00:00')),\
       'EST_IM_COST_TO_DATE'] = 500000 #LAMB RANCH
    df.loc[(df.INC_IDENTIFIER == 634790) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-07-19 19:00:00')),\
           'EST_IM_COST_TO_DATE'] = 450000 #Hurricane Creek
    df.loc[(df.INC_IDENTIFIER == 702211) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-09-02 10:00:00')),\
           'EST_IM_COST_TO_DATE'] = 8755000 #SAND
    df.loc[(df.INC_IDENTIFIER == 803156) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-09-01 20:00:00')),\
           'EST_IM_COST_TO_DATE'] = 1500 #Vance
    df.loc[(df.INC_IDENTIFIER == 808974) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-09-01 20:00:00')),\
           'EST_IM_COST_TO_DATE'] = 1000 #Jerusalem (Same update time as above)
    df.loc[(df.INC_IDENTIFIER == 808990) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-09-16 14:15:00')),\
           'EST_IM_COST_TO_DATE'] = 1000 #Eagle Creek
    df.loc[(df.INC_IDENTIFIER == 883655) & (df.REPORT_FROM_DATE == pd.Timestamp('2014-09-01 20:00:00')),\
           'EST_IM_COST_TO_DATE'] = 1000 #Nick Wynn
    df.loc[(df.EVENT_ID == 'AK-TAS-313145|2013|1') & (df.REPORT_TO_DATE == '2013-10-23 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 357776 #Chisana River
    df.loc[(df.EVENT_ID == 'AK-UYD-000046|2004|1') & (df.REPORT_TO_DATE == '2004-09-03 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 9500000 #Central Complex
    df.loc[(df.EVENT_ID == 'AZ-A3S-080440|2008|1') & (df.REPORT_TO_DATE == '2008-06-24 20:00:00'),\
           'EST_IM_COST_TO_DATE'] = 40000 #Adams
    df.loc[(df.EVENT_ID == 'AZ-CNF-000033|2009|1') & (df.REPORT_TO_DATE == '2009-05-13 11:45:00'),\
           'EST_IM_COST_TO_DATE'] = 180000 #Irishman
    df.loc[(df.EVENT_ID == 'AZ-FTA-0060|2012|1') & (df.REPORT_TO_DATE == '2012-05-31 8:00:00'),\
           'EST_IM_COST_TO_DATE'] = 3000000 #Bull Flat Fire
    df.loc[(df.EVENT_ID == 'AZ-GCP-000262|2004|1') & (df.REPORT_TO_DATE == '2004-11-19 15:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2000000 #Marble-Jim Complex
    df.loc[(df.EVENT_ID == 'AZ-GCP-0134|2003|1') & (df.REPORT_TO_DATE == '2003-12-28 0:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2500000 #Poplar Complex
    df.loc[(df.EVENT_ID == 'AZ-PHD-60171|2006|1') & (df.REPORT_TO_DATE == '2006-03-13 12:00:00'),\
           'EST_IM_COST_TO_DATE'] = 50000 #SADDLE
    df.loc[(df.EVENT_ID == 'AZ-PNF-000362|2005|1') & (df.REPORT_TO_DATE == '2005-08-02 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 950000 #BUTTE
    df.loc[(df.EVENT_ID == 'AZ-TNF-097|2003|1') & (df.REPORT_TO_DATE == '2003-07-03 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 4250000 #Picture
    df.loc[(df.EVENT_ID == 'CA-BDF-000557|2012|1') & (df.REPORT_TO_DATE == '2012-06-09 15:27:00'),\
           'EST_IM_COST_TO_DATE'] = 284000 #Lawler
    df.loc[(df.EVENT_ID == 'CA-BDU-005802|2013|1') & (df.REPORT_TO_DATE == '2013-05-14 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 200000 #Lytle
    df.loc[(df.EVENT_ID == 'CA-BDU-009640|2009|1') & (df.REPORT_TO_DATE == '2009-09-07 17:50:00'),\
           'EST_IM_COST_TO_DATE'] = 1604503 #Pendleton
    df.loc[(df.EVENT_ID == 'CA-ENF-018041|2012|1') & (df.REPORT_TO_DATE == '2012-08-08 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 300000 #HAZEL
    df.loc[(df.EVENT_ID == 'CA-FHL-002679|2013|1') & (df.REPORT_TO_DATE == '2013-08-26 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 300000 #Mission
    df.loc[(df.EVENT_ID == 'CA-HUU-004435|2003|1') & (df.REPORT_TO_DATE == '2003-10-31 10:00:00'),\
           'EST_IM_COST_TO_DATE'] = 33900000 #Canoe/Honeydew
    df.loc[(df.EVENT_ID == 'CA-KNF-003497|2006|1') & (df.REPORT_TO_DATE == '2006-10-23 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 15516000 #Uncles Complex
    df.loc[(df.EVENT_ID == 'CA-LPF-001249|2011|1') & (df.REPORT_TO_DATE == '2011-08-12 17:15:00'),\
           'EST_IM_COST_TO_DATE'] = 1000000 #RANGE
    df.loc[(df.EVENT_ID == 'CA-MCP-362|2012|1') & (df.REPORT_TO_DATE == '2012-04-04 9:00:00'),\
           'EST_IM_COST_TO_DATE'] = 100000 #RANGE (different inc#)
    df.loc[(df.EVENT_ID == 'CA-MDF-000456|2013|1') & (df.REPORT_TO_DATE == '2013-07-29 16:15:00'),\
           'EST_IM_COST_TO_DATE'] = 20000 #Garden Complex
    df.loc[(df.EVENT_ID == 'CA-MDF-0275|2005|1') & (df.REPORT_TO_DATE == '2005-08-30 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 50000 #COUGAR
    df.loc[(df.EVENT_ID == 'CA-MEU-5879|2003|1') & (df.REPORT_TO_DATE == '2003-08-04 8:00:00'),\
           'EST_IM_COST_TO_DATE'] = 600000 #RATTLESNAKE
    df.loc[(df.EVENT_ID == 'CA-MVU-010432|2007|1') & (df.REPORT_TO_DATE == '2007-11-06 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 18000000 #Witch
    df.loc[(df.EVENT_ID == 'CA-MVU-5659|2004|1') & (df.REPORT_TO_DATE == '2004-07-18 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 1900000 #Mataguay
    df.loc[(df.EVENT_ID == 'CA-SHU-006398|2004|1') & (df.REPORT_TO_DATE == '2004-09-05 19:00:00'),\
           'EST_IM_COST_TO_DATE'] = 18816443 #French
    df.loc[(df.EVENT_ID == 'CA-TIA-3438|2013|1') & (df.REPORT_TO_DATE == '2013-09-05 17:00:00'),\
           'EST_IM_COST_TO_DATE'] = 300000 #WINDY
    df.loc[(df.EVENT_ID == 'CA-VNC-11-0039839|2011|1') & (df.REPORT_TO_DATE == '2011-07-16 13:42:00'),\
           'EST_IM_COST_TO_DATE'] = 100000 #COLLINS
    df.loc[(df.EVENT_ID == 'CA-YNP-1962|2004|1') & (df.REPORT_TO_DATE == '2004-10-20 12:00:00'),\
           'EST_IM_COST_TO_DATE'] = 1500000 #Meadow WFU Complex
    df.loc[(df.EVENT_ID == 'CO-PSF-000683|2005|1') & (df.REPORT_TO_DATE == '2005-07-30 10:34:00'),\
           'EST_IM_COST_TO_DATE'] = 5500000 #Mason
    df.loc[(df.EVENT_ID == 'CO-SJF-000213|2011|1') & (df.REPORT_TO_DATE == '2011-08-25 15:00:00'),\
           'EST_IM_COST_TO_DATE'] = 12000 #Wildhorse
    df.loc[(df.EVENT_ID == 'FL-EAQ-120001|2012|1') & (df.REPORT_TO_DATE == '2012-07-30 14:30:00'),\
           'EST_IM_COST_TO_DATE'] = 200000 #Black Bear
    df.loc[(df.EVENT_ID == 'ID-LEX-012127|2012|1') & (df.REPORT_TO_DATE == '2012-07-30 14:30:00'),\
           'EST_IM_COST_TO_DATE'] = 200000 #Tenmile
    df.loc[(df.EVENT_ID == 'ID-PAF-000063|2003|1') & (df.REPORT_TO_DATE == '2003-11-07 11:10:00'),\
           'EST_IM_COST_TO_DATE'] = 10000 #Marble
    df.loc[(df.EVENT_ID == 'ID-PAF-008|2000|1') & (df.REPORT_TO_DATE == '2000-10-03'),\
           'EST_IM_COST_TO_DATE'] = 23600000 #BURGDORF JUNCTION
    df.loc[(df.EVENT_ID == 'ID-SCF-009178|2009|1') & (df.REPORT_TO_DATE == '2009-08-04 19:00:00'),\
           'EST_IM_COST_TO_DATE'] = 1139630 #GOODLUCK
    df.loc[(df.EVENT_ID == 'ID-SCF-1019|2001|1') & (df.REPORT_TO_DATE == '2001-10-29 15:00:00'),\
           'EST_IM_COST_TO_DATE'] = 3456600 #Potter Vine
    df.loc[(df.EVENT_ID == 'ID-TFD-000281|2011|1') & (df.REPORT_TO_DATE == '2011-09-03 15:39:00'),\
           'EST_IM_COST_TO_DATE'] = 2000000 #Roseworth
    df.loc[(df.EVENT_ID == 'KY-DBF-100077|2010|1') & (df.REPORT_TO_DATE == '2010-11-04 20:24:00'),\
           'EST_IM_COST_TO_DATE'] = 225000 #Fish Trap
    df.loc[(df.EVENT_ID == 'LA-SBR-011006|2011|1') & (df.REPORT_TO_DATE == '2011-07-23 13:00:00'),\
           'EST_IM_COST_TO_DATE'] = 65000 #Cocodrie
    df.loc[(df.EVENT_ID == 'LA-SBR-43640.2|2000|1') & (df.REPORT_TO_DATE == '2000-07-15'),\
           'EST_IM_COST_TO_DATE'] = 6000 #Overboard
    df.loc[(df.EVENT_ID == 'MN-SUF-000032|2004|1') & (df.REPORT_TO_DATE == '2004-09-15 8:00:00'),\
           'EST_IM_COST_TO_DATE'] = 603890 #SUPERIOR NF RX 04
    df.loc[(df.EVENT_ID == 'MO-MTF-00064|2011|1') & (df.REPORT_TO_DATE == '2011-04-12 14:40:00'),\
           'EST_IM_COST_TO_DATE'] = 50000 #Blair
    df.loc[(df.EVENT_ID == 'MO-MTF-00064|2011|1') & (df.REPORT_TO_DATE == '2013-08-14 18:55:00'),\
           'EST_IM_COST_TO_DATE'] = 100000 #INDIAN CREEK
    df.loc[(df.EVENT_ID == 'MT-EAS-84012|2003|1') & (df.REPORT_TO_DATE == '2003-08-15 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 20000 #Cow Creek
    df.loc[(df.EVENT_ID == 'MT-FHA-273|2006|1') & (df.REPORT_TO_DATE == '2006-08-07 20:11:00'),\
           'EST_IM_COST_TO_DATE'] = 200000 #Irvine
    df.loc[(df.EVENT_ID == 'MT-GNF-020|2007|1') & (df.REPORT_TO_DATE == '2007-07-06 17:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2434000 #Madison Arm
    df.loc[(df.EVENT_ID == 'MT-LNF-000539|2005|1') & (df.REPORT_TO_DATE == '2005-10-05 8:30:00'),\
           'EST_IM_COST_TO_DATE'] = 3354250 #PROSPECT
    df.loc[(df.EVENT_ID == 'MT-MCD-096|2007|1') & (df.REPORT_TO_DATE == '2007-08-21 17:10:00'),\
           'EST_IM_COST_TO_DATE'] = 2300000 #POWDER RIVER
    df.loc[(df.EVENT_ID == 'MT-MCD-137|2001|1') & (df.REPORT_TO_DATE == '2001-08-29 0:00:00'),\
           'EST_IM_COST_TO_DATE'] = 303000 #NORTH SAWMILL
    df.loc[(df.EVENT_ID == 'NC-NCF-075008|2007|1') & (df.REPORT_TO_DATE == '2007-07-23 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 4960000 #Linville Complex
    df.loc[(df.EVENT_ID == 'NE-NBF-120653|2012|1') & (df.REPORT_TO_DATE == '2012-07-20 19:20:00'),\
           'EST_IM_COST_TO_DATE'] = 85000 #Cactus
    df.loc[(df.EVENT_ID == 'NM-CIF-000117|2007|1') & (df.REPORT_TO_DATE == '2007-10-16 17:30:00'),\
           'EST_IM_COST_TO_DATE'] = 800000 #Agua
    df.loc[(df.EVENT_ID == 'NM-GNF-000705|2011|1') & (df.REPORT_TO_DATE == '2011-08-19 18:45:00'),\
           'EST_IM_COST_TO_DATE'] = 60000 #Eagle Fire
    df.loc[(df.EVENT_ID == 'NV-ELD-000041|2012|1') & (df.REPORT_TO_DATE == '2012-06-10 17:35:00'),\
           'EST_IM_COST_TO_DATE'] = 2500000 #White Rock
    df.loc[(df.EVENT_ID == 'NV-ELD-000041|2012|1') & (df.REPORT_TO_DATE == '2012-06-10 17:35:00'),\
           'EST_IM_COST_TO_DATE'] = 2500000 #White Rock
    df.loc[(df.EVENT_ID == 'NV-HTF-101252|2012|1') & (df.REPORT_TO_DATE == '2012-08-18 17:00:00'),\
           'EST_IM_COST_TO_DATE'] = 150000 #West Marys
    df.loc[(df.EVENT_ID == 'NV-LVD-000043|2005|1') & (df.REPORT_TO_DATE == '2005-07-06 0:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2160000 #GOOD SPRINGS
    df.loc[(df.EVENT_ID == 'NV-SND-500028|2012|1') & (df.REPORT_TO_DATE == '2012-04-30 15:51:00'),\
           'EST_IM_COST_TO_DATE'] = 100000 #WHITE ROCK
    df.loc[(df.EVENT_ID == 'OK-NEU-12-30004|2012|1') & (df.REPORT_TO_DATE == '2012-01-06 16:20:00'),\
           'EST_IM_COST_TO_DATE'] = 13000 #Gobbler Ridge
    df.loc[(df.EVENT_ID == 'OR-BUD-003172|2013|1') & (df.REPORT_TO_DATE == '2012-01-06 16:20:00'),\
           'EST_IM_COST_TO_DATE'] = 2500000 #House Creek
    df.loc[(df.EVENT_ID == 'OR-UPF-008083|2008|1') & (df.REPORT_TO_DATE == '2008-09-06 16:30:00'),\
           'EST_IM_COST_TO_DATE'] = 2500000 #Rattle Fire North Fork
    df.loc[(df.EVENT_ID == 'OR-VAD-|2001|1') & (df.REPORT_TO_DATE == '2001-08-21'),\
           'EST_IM_COST_TO_DATE'] = 316000 #Baker Complex
    df.loc[(df.EVENT_ID == 'OR-WWF-0875|2011|1') & (df.REPORT_TO_DATE == '2011-09-28 11:45:00'),\
           'EST_IM_COST_TO_DATE'] = 2200000 #Cactus Mountain
    df.loc[(df.EVENT_ID == 'SD-SDS-060649|2006|1') & (df.REPORT_TO_DATE == '2006-07-21 20:52:00'),\
           'EST_IM_COST_TO_DATE'] = 20000 #BLACK HORSE CREEK
    df.loc[(df.EVENT_ID == 'UT-BRS-000221|2007|1') & (df.REPORT_TO_DATE == '2007-06-21 11:11:00'),\
           'EST_IM_COST_TO_DATE'] = 210000 #Hansel Valley 1
    df.loc[(df.EVENT_ID == 'UT-BRS-000664|2012|1') & (df.REPORT_TO_DATE == '2012-08-08 20:30:00'),\
           'EST_IM_COST_TO_DATE'] = 192000 #Pine Canyon
    df.loc[(df.EVENT_ID == 'UT-DIF-047|2002|1') & (df.REPORT_TO_DATE == '2002-08-19 17:00:00'),\
           'EST_IM_COST_TO_DATE'] = 3080000 #Sequoia
    df.loc[(df.EVENT_ID == 'UT-FIF-000281|2011|1') & (df.REPORT_TO_DATE == '2011-09-01 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 3000 #Loafers
    df.loc[(df.EVENT_ID == 'UT-MLF1-000510|2006|1') & (df.REPORT_TO_DATE == '2011-09-01 16:00:00'),\
           'EST_IM_COST_TO_DATE'] = 1500000 #Deep Creek
    df.loc[(df.EVENT_ID == 'UT-NWS-000052|2008|1') & (df.REPORT_TO_DATE == '2008-10-31 16:45:00'),\
           'EST_IM_COST_TO_DATE'] = 184000 #Boiler
    df.loc[(df.EVENT_ID == 'UT-RID-0095|2003|1') & (df.REPORT_TO_DATE == '2003-09-10 18:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2130000 #BULLDOG
    df.loc[(df.EVENT_ID == 'UT-SWS-WS-012|2003|1') & (df.REPORT_TO_DATE == '2003-07-09 19:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2200000 #APEX
    df.loc[(df.EVENT_ID == 'WA-COA-094|2001|1') & (df.REPORT_TO_DATE == '2001-07-14'),\
           'EST_IM_COST_TO_DATE'] = 715000 #The Dam Tower
    df.loc[(df.EVENT_ID == 'WA-NCP-112|2007|1') & (df.REPORT_TO_DATE == '2007-12-08 12:54:00'),\
           'EST_IM_COST_TO_DATE'] = 2100000 #Tolo
    df.loc[(df.EVENT_ID == 'WA-OWF-000642|2012|1') & (df.REPORT_TO_DATE == '2012-10-19 15:25:00'),\
           'EST_IM_COST_TO_DATE'] = 19500000 #Table Mountain
    df.loc[(df.EVENT_ID == 'WA-SES-489|2009|1') & (df.REPORT_TO_DATE == '2009-07-20 13:00:00'),\
           'EST_IM_COST_TO_DATE'] = 1760000 #FORREST
    df.loc[(df.EVENT_ID == 'WA-YAA-108|2012|1') & (df.REPORT_TO_DATE == '2012-08-30 19:00:00'),\
           'EST_IM_COST_TO_DATE'] = 2000000 #DIAMOND BUTTE
    df.loc[(df.EVENT_ID == 'WY-RAD-2012 - 269|2012|1') & (df.REPORT_TO_DATE == '2012-07-29 18:15:00'),\
           'EST_IM_COST_TO_DATE'] = 300000 #Ferris
    
    
    return df

def _event_forward_fill(df):
    rows = df.shape[0]
    df = df.copy()
    print("Forward fill cost estimates & acres fields...")
    #for i in range(0,25): #test loop
    for i in range(0,rows-1):
        if (i%1000 == 0):
            print("row #: {}".format(i))
        if df.iloc[i].EVENT_ID == df.iloc[i+1].EVENT_ID: #curr and next part of same incident
            if  np.isnan(df.iloc[i+1].EST_IM_COST_TO_DATE) and np.isfinite(df.iloc[i].EST_IM_COST_TO_DATE):
                df.EST_IM_COST_TO_DATE.iloc[i+1] = df.iloc[i].EST_IM_COST_TO_DATE
            if  np.isnan(df.iloc[i+1].PROJECTED_FINAL_IM_COST) and np.isfinite(df.iloc[i].PROJECTED_FINAL_IM_COST):
                df.PROJECTED_FINAL_IM_COST.iloc[i+1] = df.iloc[i].PROJECTED_FINAL_IM_COST
            if  np.isnan(df.iloc[i+1].ACRES) and np.isfinite(df.iloc[i].ACRES):
                df.ACRES.iloc[i+1] = df.iloc[i].ACRES 
            if  np.isnan(df.iloc[i+1].STR_DAMAGED) and np.isfinite(df.iloc[i].STR_DAMAGED):
                df.STR_DAMAGED.iloc[i+1] = df.iloc[i].STR_DAMAGED
            if  np.isnan(df.iloc[i+1].STR_DESTROYED) and np.isfinite(df.iloc[i].STR_DESTROYED):
                df.STR_DESTROYED.iloc[i+1] = df.iloc[i].STR_DESTROYED
            if  np.isnan(df.iloc[i+1].STR_DAMAGED_COMM) and np.isfinite(df.iloc[i].STR_DAMAGED_COMM):
                df.STR_DAMAGED_COMM.iloc[i+1] = df.iloc[i].STR_DAMAGED_COMM
            if  np.isnan(df.iloc[i+1].STR_DESTROYED_COMM) and np.isfinite(df.iloc[i].STR_DESTROYED_COMM):
                df.STR_DESTROYED_COMM.iloc[i+1] = df.iloc[i].STR_DESTROYED_COMM
            if  np.isnan(df.iloc[i+1].STR_DAMAGED_RES) and np.isfinite(df.iloc[i].STR_DAMAGED_RES):
                df.STR_DAMAGED_RES.iloc[i+1] = df.iloc[i].STR_DAMAGED_RES
            if  np.isnan(df.iloc[i+1].STR_DESTROYED_RES) and np.isfinite(df.iloc[i].STR_DESTROYED_RES):
                df.STR_DESTROYED_RES.iloc[i+1] = df.iloc[i].STR_DESTROYED_RES
            if  np.isnan(df.iloc[i+1].POO_LATITUDE) and np.isfinite(df.iloc[i].POO_LATITUDE):
                df.POO_LATITUDE.iloc[i+1] = df.iloc[i].POO_LATITUDE
            if  np.isnan(df.iloc[i+1].POO_LONGITUDE) and np.isfinite(df.iloc[i].POO_LONGITUDE):
                df.POO_LONGITUDE.iloc[i+1] = df.iloc[i].POO_LONGITUDE
    df.STR_DAMAGED.fillna(0,inplace=True)
    df.STR_DAMAGED_RES.fillna(0,inplace=True)
    df.STR_DAMAGED_COMM.fillna(0,inplace=True)
    df.STR_DESTROYED.fillna(0,inplace=True)
    df.STR_DESTROYED_RES.fillna(0,inplace=True)
    df.STR_DESTROYED_COMM.fillna(0,inplace=True)
    df.ACRES.fillna(0,inplace=True)  
    return df

def _event_smoothing_pass(df):
    rows = df.shape[0]
    df['NEW_ACRES'] = 0.0
    df['REPORT_DAY_SPAN'] = 0
    # Backward smoothing
    print("Backward smoothing pass...")
    #for i in range(25,0,-1): # test loop
    for i in range(rows-1,0,-1):
        if (i%1000 == 0):
            print("row #: {}".format(i))
        if df.iloc[i].EVENT_ID == df.iloc[i-1].EVENT_ID: # same incident
            if np.isfinite(df.iloc[i].EST_IM_COST_TO_DATE) and np.isfinite(df.iloc[i-1].EST_IM_COST_TO_DATE): #not nan
                currEstCost = df.iloc[i].EST_IM_COST_TO_DATE
                prevEstCost = df.iloc[i-1].EST_IM_COST_TO_DATE
                while prevEstCost/10 >= currEstCost: # order of magnitude larger 
                    prevEstCost = prevEstCost/10 #Reduce by order of magnitude by dividing by 10
                    df.EST_IM_COST_TO_DATE.iloc[i-1] = prevEstCost
            if np.isfinite(df.iloc[i].PROJECTED_FINAL_IM_COST) and np.isfinite(df.iloc[i-1].PROJECTED_FINAL_IM_COST): #not nan
                currProjCost = df.iloc[i].PROJECTED_FINAL_IM_COST
                prevProjCost = df.iloc[i-1].PROJECTED_FINAL_IM_COST
                while prevProjCost/10 >= currProjCost: # order of magnitude larger
                    prevProjCost = prevProjCost/10 #Reduce by order of magnitude by dividing by 10
                    df.PROJECTED_FINAL_IM_COST.iloc[i-1] = prevProjCost
            if (np.isfinite(df.iloc[i].ACRES) & np.isfinite(df.iloc[i-1].ACRES)) & (df.iloc[i-1].ACRES > df.iloc[i].ACRES):
                df['ACRES'].iloc[i-1] = df.iloc[i].ACRES # adjust acres down
            if np.isfinite(df.iloc[i].ACRES) & np.isfinite(df.iloc[i-1].ACRES):
                df['NEW_ACRES'].iloc[i] = df.iloc[i].ACRES - df.iloc[i-1].ACRES
                df['REPORT_DAY_SPAN'].iloc[i] = df.iloc[i].REPORT_DOY - df.iloc[i-1].REPORT_DOY
        else: # at the boundary of even
            df['NEW_ACRES'].iloc[i] = df.iloc[i].ACRES
            df['REPORT_DAY_SPAN'].iloc[i] = df.iloc[i].REPORT_DOY - df.iloc[i].DISCOVERY_DOY 
    # set values for row 0
    df['NEW_ACRES'].iloc[0] = df.iloc[0].ACRES
    df['REPORT_DAY_SPAN'].iloc[0] = df.iloc[0].REPORT_DOY - df.iloc[0].DISCOVERY_DOY
    
    
    # forward smoothing
    print("Forward smoothing pass...")
    #for i in range(0,25): # test loop
    for i in range(0,rows-1):
        if (i%1000 == 0):
            print("row #: {}".format(i))
        if df.iloc[i].EVENT_ID == df.iloc[i+1].EVENT_ID: #curr and next part of same incident
            currEstCost = df.iloc[i].EST_IM_COST_TO_DATE
            nextEstCost = df.iloc[i+1].EST_IM_COST_TO_DATE
            if np.isfinite(currEstCost) and np.isfinite(nextEstCost):
                while nextEstCost*9.0 <= currEstCost : #allow for number to be slightly larger
                    nextEstCost = nextEstCost*10 #Make 10x larger
                    df.EST_IM_COST_TO_DATE.iloc[i+1] = nextEstCost
            currProjCost = df.iloc[i].PROJECTED_FINAL_IM_COST
            nextProjCost = df.iloc[i+1].PROJECTED_FINAL_IM_COST
            if np.isfinite(currProjCost) and np.isfinite(nextProjCost):
                while nextProjCost*9.0 <= currProjCost : #allow for number to be slightly larger
                    nextProjCost = nextProjCost*10 #Make 10x larger
                    df.PROJECTED_FINAL_IM_COST.iloc[i+1] = nextProjCost
                    
    df['PROJECTED_FINAL_IM_COST'] = df.loc[df.EST_IM_COST_TO_DATE > df.PROJECTED_FINAL_IM_COST, \
                                           'PROJECTED_FINAL_IM_COST'] = df.EST_IM_COST_TO_DATE
    #df = df.drop(['EVENT_ID'],axis=1)
    return df
        
def _other_field_smoothing(df):
    df = df.copy()
    df = df.sort_values(['INCIDENT_ID','REPORT_TO_DATE'])
    df = df.reset_index(drop=True)
    
    df.FATALITIES.replace(0, np.nan,inplace=True)
    df.INJURIES_TO_DATE.replace(0, np.nan,inplace=True)
    
    
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE)
    df['DISCOVERY_DATE'] = pd.to_datetime(df.DISCOVERY_DATE)
    
    # Forward fill wildfire fields and calculate stats
    print("Cleaning other fields...")
    for i in range(0,df.shape[0]-1):
        if (i%1000 == 0):
            print("row #: {}".format(i))
        if (df.iloc[i].INCIDENT_ID == df.iloc[i+1].INCIDENT_ID) & ((df.iloc[i].INCTYP_ABBREVIATION == "WF") or \
                                                                   (df.iloc[i].INCTYP_ABBREVIATION == "WFU")): 
                                                                    #curr and next part of same incident
            if  np.isnan(df.iloc[i+1].FATALITIES) and np.isfinite(df.iloc[i].FATALITIES):
                df.FATALITIES.iloc[i+1] = df.iloc[i].FATALITIES
                #print("{}({}) Null FATALITIES found, setting next to {}".format(i,df.iloc[i].INCIDENT_ID,df.iloc[i].FATALITIES))
            if  np.isnan(df.iloc[i+1].INJURIES_TO_DATE) and np.isfinite(df.iloc[i].INJURIES_TO_DATE):
                df.INJURIES_TO_DATE.iloc[i+1] = df.iloc[i].INJURIES_TO_DATE
    df.FATALITIES.fillna(0,inplace=True)
    df.INJURIES.fillna(0,inplace=True)
    df.INJURIES_TO_DATE.fillna(0,inplace=True)
    
    return df

def _calculate_fire_statistics(df):
    
    e_grp_final = df.sort_values(['INCIDENT_ID','FIRE_EVENT_ID',\
                                    'REPORT_TO_DATE']).groupby(['INCIDENT_ID','FIRE_EVENT_ID']).nth(-1)
    e_grp_final = e_grp_final.reset_index()
    e_grp_final = e_grp_final.drop(e_grp_final[(e_grp_final.EVENT_ID == "2014.0|WA-WFS-513|1")].index)
    e_grp_final = e_grp_final[['INCIDENT_ID','FIRE_EVENT_ID','ACRES']].copy()
    e_grp_final.columns = ['INCIDENT_ID','FIRE_EVENT_ID','EVENT_FINAL_ACRES']

    # select the biggest acre event
    e_cpx_max = e_grp_final.groupby(['INCIDENT_ID'])['EVENT_FINAL_ACRES'].max().reset_index()
    e_cpx_max.columns = ['INCIDENT_ID','FIRE_MAX_ACRES']

    df = pd.merge(df,e_grp_final, on=['INCIDENT_ID','FIRE_EVENT_ID'], how='left')
    df = pd.merge(df,e_cpx_max, on=['INCIDENT_ID'],how='left')
    df.loc[df.REPORT_DAY_SPAN == 0.0,'REPORT_DAY_SPAN'] = 1.0
    df['WF_FSR'] = df.NEW_ACRES/df.REPORT_DAY_SPAN
    # calculate % relative final size
    #df.loc[df.ACRES.notnull() & df.EVENT_FINAL_ACRES.notnull(),'EVENT_PCT_FINAL_SIZE'] = df.ACRES/df.EVENT_FINAL_ACRES
    df.loc[df.ACRES.notnull() & df.FIRE_MAX_ACRES.notnull(),'MAX_FIRE_PCT_FINAL_SIZE'] = df.ACRES/df.FIRE_MAX_ACRES
    #df = df.drop(['EVENT_FINAL_ACRES'],axis=1)
    df = df.drop(['FIRE_MAX_ACRES'],axis=1)
        
    return df

def final_drop_extra_columns(df):
    df = df.drop(['EVENT_ID',
                  'MAX_FIRE_PCT_FINAL_SIZE'
                 ],axis=1)
 
    return df

def _create_incident_summary(wfdf):
    
    # Get first row of incident for key initial values
    wfinc_1st = wfdf.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby(['INCIDENT_ID']).first()
                                                                            
    # Get last row for final report values
    wfinc_df = wfdf.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby('INCIDENT_ID').nth(-1)
    wfinc_df = wfinc_df.reset_index()
    
    # take a subset of columns from final values
    wfincdf = wfinc_df[['INCIDENT_ID','INCIDENT_NUMBER','INCIDENT_NAME','INCTYP_ABBREVIATION',\
                    'ACRES','CAUSE','COMPLEX','DISCOVERY_DATE','DISCOVERY_DOY','EXPECTED_CONTAINMENT_DATE',
                    'FATALITIES','FUEL_MODEL','INCIDENT_DESCRIPTION','INC_IDENTIFIER','INJURIES_TO_DATE',\
                    'LL_CONFIDENCE','LL_UPDATE','LOCAL_TIMEZONE','POO_CITY','POO_COUNTY','POO_LATITUDE',\
                    'POO_LONGITUDE','POO_SHORT_LOCATION_DESC','POO_STATE','PROJECTED_FINAL_IM_COST','START_YEAR',\
                    'SUPPRESSION_METHOD','STR_DAMAGED','STR_DAMAGED_COMM','STR_DAMAGED_RES','STR_DESTROYED',\
                    'STR_DESTROYED_COMM','STR_DESTROYED_RES','TOTAL_R_FATALITIES','TOTAL_P_FATALITIES',\
                    'REPORT_TO_DATE','INCIDENT_ID_OLD']].copy()
    
    # rename some of final reported totals
    wfincdf.columns = wfincdf.columns.str.replace('ACRES','FINAL_ACRES')
    wfincdf.columns = wfincdf.columns.str.replace('REPORT_TO_DATE','FINAL_REPORT_DATE')
    wfincdf.columns = wfincdf.columns.str.replace('INJURIES_TO_DATE','INJURIES_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DAMAGED','STR_DAMAGED_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DAMAGED_TOTAL_COMM','STR_DAMAGED_COMM_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DAMAGED_TOTAL_RES','STR_DAMAGED_RES_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DESTROYED','STR_DESTROYED_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DESTROYED_TOTAL_COMM','STR_DESTROYED_COMM_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('STR_DESTROYED_TOTAL_RES','STR_DESTROYED_RES_TOTAL')
    wfincdf.columns = wfincdf.columns.str.replace('TOTAL_R_FATALITIES','FATALITIES_RESPONDER')
    wfincdf.columns = wfincdf.columns.str.replace('TOTAL_P_FATALITIES','FATALITIES_PUBLIC')
    
    # calculate #inc mgmt number of sitreps and merge into incident summary record
    num_rpts = wfdf.groupby(['INCIDENT_ID']).size().reset_index(name='INC_MGMT_NUM_SITREPS')
    wfincdf = pd.merge(wfincdf,num_rpts,on=['INCIDENT_ID'],how='left')
    
    # calculate maximum evacuations & 
    # TODO: Set Peak evacuation DOY/Date
    max_evac = wfdf.groupby(['INCIDENT_ID']).RPT_EVACUATIONS.max().reset_index(name='PEAK_EVACUATIONS')
    wfincdf = pd.merge(wfincdf,max_evac,on=['INCIDENT_ID'],how='left')
    
    # max structures threatened
    str_thr = wfdf.groupby(['INCIDENT_ID']).STR_THREATENED.max().reset_index(name='STR_THREATENED_MAX')
    wfincdf = pd.merge(wfincdf,str_thr,on=['INCIDENT_ID'],how='left')
    wfincdf.STR_THREATENED_MAX.replace(0, np.nan,inplace=True)
    str_thr_comm = wfdf.groupby(['INCIDENT_ID']).STR_THREATENED_COMM.max().reset_index(name='STR_THREATENED_COMM_MAX')
    wfincdf = pd.merge(wfincdf,str_thr_comm,on=['INCIDENT_ID'],how='left')
    wfincdf.STR_THREATENED_COMM_MAX.replace(0, np.nan,inplace=True)
    str_thr_res = wfdf.groupby(['INCIDENT_ID']).STR_THREATENED_RES.max().reset_index(name='STR_THREATENED_RES_MAX')
    wfincdf = pd.merge(wfincdf,str_thr_res,on=['INCIDENT_ID'],how='left')
    wfincdf.STR_THREATENED_RES_MAX.replace(0, np.nan,inplace=True)
    
    # aerial & personnel totals and peaks
    wfdf['TOTAL_AERIAL'] = wfdf.TOTAL_AERIAL.fillna(value=0.0)
    aerial = wfdf.groupby(['INCIDENT_ID']).TOTAL_AERIAL.sum().reset_index(name='TOTAL_AERIAL_SUM')
    wfincdf = pd.merge(wfincdf,aerial,on=['INCIDENT_ID'],how='left')

    wfdf['TOTAL_PERSONNEL'] = wfdf.TOTAL_PERSONNEL.fillna(value=0.0)
    pers = wfdf.groupby(['INCIDENT_ID']).TOTAL_PERSONNEL.sum().reset_index(name='TOTAL_PERSONNEL_SUM')
    wfincdf = pd.merge(wfincdf,pers,on=['INCIDENT_ID'],how='left')

    wfincdf.loc[wfincdf.TOTAL_PERSONNEL_SUM == 0.0,'TOTAL_PERSONNEL_SUM'] = np.nan
    wfincdf.loc[wfincdf.TOTAL_AERIAL_SUM == 0.0,'TOTAL_AERIAL_SUM'] = np.nan

    
    maxaerl = wfdf.groupby(['INCIDENT_ID']).TOTAL_AERIAL.max().reset_index(name='WF_PEAK_AERIAL')
    wfincdf = pd.merge(wfincdf,maxaerl,on=['INCIDENT_ID'],how='left')
    wfincdf.loc[wfincdf.WF_PEAK_AERIAL == 0.0,'WF_PEAK_AERIAL'] = np.nan
    wfdf = pd.merge(wfdf,maxaerl, on=['INCIDENT_ID'], how='left')
    maxgrowth = wfdf.loc[wfdf.TOTAL_AERIAL == wfdf.WF_PEAK_AERIAL].sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby(['INCIDENT_ID']).REPORT_TO_DATE.first().reset_index(name='WF_PEAK_AERIAL_DATE')
    wfincdf = pd.merge(wfincdf,maxgrowth,on=['INCIDENT_ID'],how='left')
    wfincdf['WF_PEAK_AERIAL_DATE'] = pd.to_datetime(wfincdf.WF_PEAK_AERIAL_DATE)
    wfincdf['WF_PEAK_AERIAL_DOY'] = wfincdf.WF_PEAK_AERIAL_DATE.dt.dayofyear

    maxpers = wfdf.groupby(['INCIDENT_ID']).TOTAL_PERSONNEL.max().reset_index(name='WF_PEAK_PERSONNEL')
    wfincdf = pd.merge(wfincdf,maxpers,on=['INCIDENT_ID'],how='left')
    wfincdf.loc[wfincdf.WF_PEAK_PERSONNEL == 0.0,'WF_PEAK_PERSONNEL'] = np.nan
    #print(wfincdf.WF_PEAK_PERSONNEL.notnull().sum()/wfincdf.shape[0])

    wfdf = pd.merge(wfdf,maxpers, on=['INCIDENT_ID'], how='left')
    maxgrowth = wfdf.loc[wfdf.TOTAL_PERSONNEL == wfdf.WF_PEAK_PERSONNEL].sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby(['INCIDENT_ID']).REPORT_TO_DATE.first().reset_index(name='WF_PEAK_PERSONNEL_DATE')
    wfincdf = pd.merge(wfincdf,maxgrowth,on=['INCIDENT_ID'],how='left')
    wfincdf['WF_PEAK_PERSONNEL_DATE'] = pd.to_datetime(wfincdf.WF_PEAK_PERSONNEL_DATE)
    wfincdf['WF_PEAK_PERSONNEL_DOY'] = wfincdf.WF_PEAK_PERSONNEL_DATE.dt.dayofyear
    
    # cessation date
    cessation = wfdf.loc[wfdf.MAX_FIRE_PCT_FINAL_SIZE >= 0.95].sort_values(['INCIDENT_ID',\
            'REPORT_TO_DATE']).groupby(['INCIDENT_ID']).REPORT_TO_DATE.first().reset_index(name='WF_CESSATION_DATE')
    cessation['WF_CESSATION_DATE'] = pd.to_datetime(cessation.WF_CESSATION_DATE)
    cessation['WF_CESSATION_DOY'] = cessation.WF_CESSATION_DATE.dt.dayofyear
    wfincdf = pd.merge(wfincdf,cessation,on=['INCIDENT_ID'],how='left')
    
    # max fire spread rate
    maxfsr = wfdf.sort_values(['INCIDENT_ID',\
                               'REPORT_TO_DATE']).groupby(['INCIDENT_ID']).WF_FSR.max().reset_index(name='WF_MAX_FSR')
    wfincdf = pd.merge(wfincdf,maxfsr,on=['INCIDENT_ID'],how='left')
    wfincdf.WF_MAX_FSR.notnull().sum()/wfincdf.shape[0]
    
    wfdf = pd.merge(wfdf,maxfsr, on=['INCIDENT_ID'], how='left')
    maxgrowth = wfdf.loc[wfdf.WF_FSR == wfdf.WF_MAX_FSR].sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby(['INCIDENT_ID']).REPORT_TO_DATE.first().reset_index(name='WF_MAX_GROWTH_DATE')                                       
    maxgrowth.head()
    wfincdf = pd.merge(wfincdf,maxgrowth,on=['INCIDENT_ID'],how='left')
    wfincdf['WF_MAX_GROWTH_DATE'] = pd.to_datetime(wfincdf.WF_MAX_GROWTH_DATE)
    wfincdf['WF_MAX_GROWTH_DOY'] = wfincdf.WF_MAX_GROWTH_DATE.dt.dayofyear
    wfincdf['WF_GROWTH_DURATION'] = wfincdf.WF_CESSATION_DOY - wfincdf.DISCOVERY_DOY
    
    # Suppression Method Values
    supm_df = wfdf[['INCIDENT_ID','REPORT_TO_DATE','SUPPRESSION_METHOD']].copy()
    supm_df.sort_values(by=['INCIDENT_ID','REPORT_TO_DATE'], inplace=True)
    
    # Create aggregated values   
    supm_df['SUP_COUNT'] = 0
    # Suppression Series and Series Length
    s_agg1 = supm_df.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby('INCIDENT_ID', 
                                                               as_index=False).agg({
                                                            'SUP_COUNT': 'count',
                                                            'SUPPRESSION_METHOD': lambda x: list(x),
                                                                }).rename(columns={
                                                            'SUP_COUNT':'SUP_SERIES_LEN',
                                                            'SUPPRESSION_METHOD':'SUP_SERIES',
                                                                })
    # Initial Suppression Method
    s_agg2 = supm_df.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby('INCIDENT_ID', 
                                                               as_index=False).agg({
                                                            'SUPPRESSION_METHOD': lambda x: list(x)[0],
                                                                }).rename(columns={
                                                            'SUPPRESSION_METHOD': 'SUP_METHOD_INITIAL',
                                                                })
    # Final Suppression Method
    s_agg3 = supm_df.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby('INCIDENT_ID', 
                                                               as_index=False).agg({
                                                            'SUPPRESSION_METHOD': lambda x: list(x)[-1],
                                                                }).rename(columns={
                                                            'SUPPRESSION_METHOD': 'SUP_METHOD_FINAL',
                                                                })
    # Count in Full Suppression
    s_agg4 = supm_df.sort_values(['INCIDENT_ID','REPORT_TO_DATE']).groupby('INCIDENT_ID', 
                                                               as_index=False).agg({
                                                            'SUPPRESSION_METHOD': lambda x: str(x).count('FS')
                                                                }).rename(columns={
                                                            'SUPPRESSION_METHOD': 'SUP_DAYS_FS',
                                                                })
    # Merge aggregations together
    sup_agg = s_agg1.merge(s_agg2, how='left')
    sup_agg = sup_agg.merge(s_agg3, how = 'left')
    sup_agg = sup_agg.merge(s_agg4, how = 'left')
    # Calculate Percent Full Suppression
    sup_agg['SUP_PERCENT_FS'] = sup_agg.SUP_DAYS_FS / sup_agg.SUP_SERIES_LEN * 100
    sup_agg.drop(['SUP_SERIES_LEN','SUP_DAYS_FS'],axis=1,inplace=True)
    # Merge with inc_df
    wfincdf = pd.merge(wfincdf,sup_agg,on=['INCIDENT_ID'],how='left')
    
    return wfincdf


def get_largest_fod_fire(fod_fire_list):
    for i in range(0,len(fod_fire_list)):
        if isinstance(fod_fire_list[i],str):
            fod_fire_list[i] = eval(fod_fire_list[i])
    df = pd.DataFrame(fod_fire_list)
    max_row = df.loc[df.SIZE == df.SIZE.max()]
    return max_row

def _fod_merge(fod_agg, inc_df):
    
    # Join fod_agg with incident data
    #fod_agg.rename(columns={"FODJ_INCIDENT_ID": "INCIDENT_ID"}, inplace=True)
    inc_df = inc_df.merge(fod_agg, on="FODJ_INCIDENT_ID", how='left')
    inc_df.to_csv(os.path.join(tmp_dir,'fod-agg-merge-out.csv'))
    
    merge_col = "FODJ_INCIDENT_ID"
     
     # Multiple fires case:
    fod_fires = inc_df.loc[inc_df.FOD_FIRE_NUM>=1].copy()
    fod_fires = fod_fires[['FODJ_INCIDENT_ID','FOD_FIRE_LIST']]
    fod_fires.to_csv(os.path.join(tmp_dir,'fod-fires.csv'))
    
    
    for i in range(0,fod_fires.shape[0]):
    #for i in range(0,4):
        max_row = get_largest_fod_fire(fod_fires.iloc[i].FOD_FIRE_LIST)
        inc_df.loc[inc_df[merge_col] == fod_fires.iloc[i][merge_col], 'LRGST_FOD_ID'] = max_row.iloc[0]['ID']
        if 'COORDS' in max_row.columns:
            inc_df.loc[inc_df[merge_col] == fod_fires.iloc[i][merge_col], 'LRGST_FOD_LATITUDE'] = max_row.iloc[0]['COORDS'][0]
            inc_df.loc[inc_df[merge_col] == fod_fires.iloc[i][merge_col], 'LRGST_FOD_LONGITUDE'] = max_row.iloc[0]['COORDS'][1]
        if 'MTBS_ID' in max_row.columns:
            inc_df.loc[inc_df[merge_col] == fod_fires.iloc[i][merge_col], 'LRGST_MTBS_FIRE_INFO'] = max_row.iloc[0]['MTBS_ID']
    inc_df['LRGST_FOD_COORDS'] = inc_df[['LRGST_FOD_LATITUDE', 'LRGST_FOD_LONGITUDE']].apply(tuple, axis=1)
    
    fod_fires.to_csv(os.path.join(tmp_dir,'fod-fire-data.csv')) 
    
    return inc_df

def set_fod_join_id(inc_df,fod_df,cpx_df):
    inc_df = inc_df.copy()
    
    mbr_dict = dict(zip(cpx_df.MEMBER_INCIDENT_ID.tolist(),cpx_df.FODJ_INCIDENT_ID.tolist()))
    
    incj_ids = fod_df['ICS_209_PLUS_INCIDENT_JOIN_ID'].to_list()
    cpxj_ids = fod_df['ICS_209_PLUS_COMPLEX_JOIN_ID'].to_list()
    mbrj_ids = cpx_df['MEMBER_INCIDENT_ID'].to_list()
    
    inc_df['inc_join'] = False
    inc_df['cpx_join'] = False
    inc_df['mbr_join'] = False
    inc_df['FODJ_INCIDENT_ID'] = ""
    # Case #1 Incident join found
    inc_df.loc[inc_df.INCIDENT_ID.isin(incj_ids),'inc_join'] = True
    inc_df.loc[inc_df.INCIDENT_ID.isin(incj_ids),'FODJ_INCIDENT_ID'] = inc_df.INCIDENT_ID
    
    # Case #2 Cpx join found
    inc_df.loc[~inc_df.inc_join & inc_df.INCIDENT_ID.isin(cpxj_ids),'cpx_join'] = True
    inc_df.loc[~inc_df.inc_join & inc_df.INCIDENT_ID.isin(cpxj_ids),'FODJ_INCIDENT_ID'] = inc_df.INCIDENT_ID
    
    # Case #3
    inc_df.loc[~inc_df.inc_join & ~inc_df.cpx_join & inc_df.INCIDENT_ID.isin(mbrj_ids), 'mbr_join'] = True
    inc_df.loc[inc_df.mbr_join,'FODJ_INCIDENT_ID'] = inc_df['INCIDENT_ID'].map(mbr_dict)
    
    return inc_df


def fod_aggregation(inc_df,cpx_df):
    print("Aggregating fod...")
    # Read FOD join dataset and prep
    fod_df = pd.read_csv(os.path.join(fod_dir,'FOD_JOIN_{}ics.csv'.format(fod_version)), 
                                      low_memory=False)
    
    inc_df = set_fod_join_id(inc_df,fod_df,cpx_df)
    cpx_df = cpx_df.copy()
    
    # Temp workaround for join issue
    fod_df['FODJ_INCIDENT_ID'] = fod_df.ICS_209_PLUS_INCIDENT_JOIN_ID
    fod_df = fod_df.loc[~fod_df.FODJ_INCIDENT_ID.isnull()]
    fod_df['FODJ_INCIDENT_ID'] = fod_df.FODJ_INCIDENT_ID.astype(str).str.strip().str.upper()
    
    # pare down FOD columns
    fod_cols = fod_df[['FOD_ID','MTBS_ID','MTBS_FIRE_NAME','NWCG_GENERAL_CAUSE',
                       'FIRE_SIZE','LATITUDE','LONGITUDE',
                       'DISCOVERY_DOY','CONT_DOY','ICS_209_PLUS_INCIDENT_JOIN_ID',
                       'FODJ_INCIDENT_ID']].copy()
    
    # create FOD Coordinates
    fod_cols['FOD_COORD_LIST'] = list(zip(fod_cols.LATITUDE,fod_cols.LONGITUDE))
    # create MTBS ID String
    fod_cols.loc[fod_cols.MTBS_ID.notnull(),'MTBS_STR'] = ", \"MTBS_ID\" : \"" + fod_cols.MTBS_ID.astype(str).str.strip() + \
                        "(" + fod_cols.MTBS_FIRE_NAME.astype(str).str.strip() + ")\""  
    fod_cols.loc[fod_cols.MTBS_ID.notnull(),'MTBS_IDS'] = fod_cols.MTBS_ID.astype(str).str.strip() + "-" + \
                                                         fod_cols.MTBS_FIRE_NAME.astype(str).str.strip()
    fod_cols.loc[fod_cols.MTBS_ID.isnull(),'MTBS_STR'] = ""
    fod_cols.loc[fod_cols.MTBS_ID.isnull(),'MTBS_IDS'] = ""
   

    fod_cols.loc[fod_cols.CONT_DOY.notnull(),'FOD_CONT_STR'] = fod_cols.CONT_DOY.astype(str)
    fod_cols.loc[fod_cols.CONT_DOY.isnull(),'FOD_CONT_STR'] = "\"unknown\""

    fod_cols.loc[fod_cols.DISCOVERY_DOY.notnull(),'FOD_DISC_STR'] = fod_cols.DISCOVERY_DOY.astype(str)
    fod_cols.loc[fod_cols.DISCOVERY_DOY.isnull(),'FOD_DISC_STR'] = "\"unknown\""
    
    
    fod_cols['FOD_STR'] = "{\"ID\" : " + fod_cols.FOD_ID.astype(str) +\
                          fod_cols.MTBS_STR +\
                          ", \"COORDS\" : " + fod_cols.FOD_COORD_LIST.astype(str) +\
                          ", \"CAUSE\" : \"" + fod_cols.NWCG_GENERAL_CAUSE.astype(str) + "\"" +\
                          ", \"SIZE\" : " + fod_cols.FIRE_SIZE.astype(str) +\
                          ", \"DISC\" : " + fod_cols.FOD_DISC_STR +\
                          ", \"CONT\" : " + fod_cols.FOD_CONT_STR +\
                          "}"
    fod_cols['FOD_COUNT'] = 0
    
    fod_agg = fod_cols.groupby('FODJ_INCIDENT_ID',as_index=False).agg({
                                            'FOD_ID': lambda x: list(x),
                                            'FOD_COUNT': 'count',
                                            'MTBS_IDS': lambda x: list(filter(None, x)),
                                            'DISCOVERY_DOY': 'min',
                                            'CONT_DOY':'max',
                                            'NWCG_GENERAL_CAUSE': lambda x: list(x),
                                            'FIRE_SIZE' : lambda x : sum(x),
                                            'FOD_STR' : lambda x: list(x),
                                            'FOD_COORD_LIST' : lambda x: list(x),
                         }).rename(columns={'FOD_ID': 'FOD_ID_LIST',
                                            'FOD_COUNT': 'FOD_FIRE_NUM',
                                            'MTBS_IDS': 'MTBS_FIRE_LIST',
                                            'DISCOVERY_DOY': 'FOD_DISCOVERY_DOY',
                                            'CONT_DOY': 'FOD_CONTAIN_DOY',
                                            'NWCG_GENERAL_CAUSE': 'FOD_CAUSE',
                                            'FIRE_SIZE' : 'FOD_FINAL_ACRES',
                                            'FOD_STR' : 'FOD_FIRE_LIST',
                                            'FOD_COORD_LIST' : 'FOD_COORD_LIST',
                                            })
    
    
    #'FOD_STR' : lambda x: "[%s]" % ', '.join(x.astype(str)),
    fod_agg['FOD_CAUSE'] = fod_agg.FOD_CAUSE.apply(lambda x: pd.unique(x))
    fod_agg['FOD_CAUSE_NUM'] = fod_agg.FOD_CAUSE.apply(lambda x: len(x))
    fod_agg['FOD_COORD_LIST'] = fod_agg.FOD_COORD_LIST.apply(lambda x: pd.unique(x).tolist())
    fod_agg['FOD_COORD_NUM'] = fod_agg['FOD_COORD_LIST'].apply(lambda x: len(x))
    fod_agg['MTBS_FIRE_LIST'] = fod_agg.MTBS_FIRE_LIST.apply(lambda x: pd.unique(x).tolist())
    fod_agg['MTBS_FIRE_NUM'] = fod_agg.MTBS_FIRE_LIST.apply(lambda x: len(x))
    fod_agg.loc[fod_agg.MTBS_FIRE_NUM == 0, 'MTBS_FIRE_LIST'] = np.nan
    #fod_agg.rename(columns={"FODJ_INCIDENT_ID": "INCIDENT_ID"}, inplace=True)
    
    fod_agg.to_csv(os.path.join(tmp_dir,'fod-agg-out.csv'))
    
    
    print("Merging incidents with FOD...")
    inc_fod = _fod_merge(fod_agg, inc_df)
    inc_fod.to_csv(os.path.join(tmp_dir,'inc-fod-out.csv'))
    
    cpx_agg = fod_agg[['FODJ_INCIDENT_ID', 'FOD_FIRE_LIST', 'MTBS_FIRE_LIST']]
    cpx_fod = cpx_df.merge(cpx_agg, on='FODJ_INCIDENT_ID', how='left')
    
    return inc_fod,cpx_fod


def _join_with_fod_database(inc_df):
    
    cpx_df = pd.read_csv(os.path.join(cpx_dir, 'cpx-assocs{}.csv'.format(final_timespan)),
                         converters = {'FIRE_NAME': str})
    cpx_df = cpx_df.loc[:, ~cpx_df.columns.str.contains('^Unnamed')]
    
    # Hard-coded fixes for INCIDENT_ID
    inc_df.loc[inc_df.INCIDENT_ID == "2015_2920368_CLEARWATER/MUNICIPAL COMPLEX",'INCIDENT_ID'] = \
                                                     "2015_2920368_CLEARWATER/MUNICIPAL/MOTORWAY NORTH COMPLEX"
    inc_df.loc[inc_df.INC_IDENTIFIER == 2882088,'INCIDENT_ID'] = \
                                                     "2015_2882088_CORNET-WINDY RIDGE"
    
    inc_fod, cpx_fod = fod_aggregation(inc_df,cpx_df)    
    inc_fod.to_csv(os.path.join(tmp_dir,'inc-fod-out.csv'))
    cpx_fod.to_csv(os.path.join(tmp_dir,'cpx-fod-out.csv'))
    
    return inc_fod,cpx_fod

def _join_with_fired_database(inc_fod):
    inc_fod = inc_fod.copy()
    fired_join = pd.read_csv(os.path.join(fired_dir,'ics209plus_fired_attributes_clean2001to2020.csv'))
    inc_fod = inc_fod.merge(fired_join,on='INCIDENT_ID',how='left')
    return inc_fod
                     
def create_final_datasets():
    
    # read cleaned version
    df_h1 = pd.read_csv(os.path.join(out_dir,'IMSR_INCIDENT_INFORMATIONS_{}_cleaned.csv'.format(lgcy_timespan)),low_memory=False)
    df_h1 = df_h1.loc[:, ~df_h1.columns.str.contains('^Unnamed')]
    print(df_h1.shape)
    df_h2 = pd.read_csv(os.path.join(out_dir,'IMSR_IMSR_209_INCIDENTS_{}_cleaned.csv'.format(hist_timespan)),low_memory=False)
    df_h2 = df_h2.loc[:, ~df_h2.columns.str.contains('^Unnamed')]
    print(df_h2.shape)
    df_curr = pd.read_csv(os.path.join(out_dir,'SIT209_HISTORY_INCIDENT_209_REPORTS_{}_cleaned.csv'.format(curr_timespan)),
                          low_memory=False)
    df_curr = df_curr.loc[:, ~df_curr.columns.str.contains('^Unnamed')]
    print(df_curr.shape)
    
    # rename columns so that matching columns align
    df_h1 = _historical1_rename_columns(df_h1)
    df_h2 = _historical2_rename_columns(df_h2)
    
    # concatenate all three datasets
    df = pd.concat([df_h1,df_h2,df_curr],sort=True)
    print(df.shape)
    
    df = _final_alignments(df)
    
    df = _drop_extra_columns(df)
    print(df.shape)
    df.to_csv(os.path.join(tmp_dir,"after_drop_{}.csv".format(final_timespan)))
    
    
    # event level smoothing
    df = _event_smoothing_prep(df)
    df = _cost_adjustments(df)
    df = _event_forward_fill(df)
    df = _event_smoothing_pass(df)
    # other field cleaning
    df = _other_field_smoothing(df)
    
    # save merged/cleaned versions
    df.to_csv(os.path.join(out_dir,'ics209-plus_sitreps_{}.csv'.format(final_timespan)))
    
    df = pd.read_csv(os.path.join(out_dir,'ics209-plus_sitreps_{}.csv'.format(final_timespan)),low_memory=False)
    
    # Wildfire dataset from here down
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    wfdf = df.loc[df.INCTYP_ABBREVIATION.isin(['WF','WFU','RX','CX'])]
    wfdf.sort_values(['INCIDENT_ID','REPORT_TO_DATE'])
    
    wfdf = _calculate_fire_statistics(wfdf)
    print("statistics calculated.")
    
    #wfdf = pd.read_csv(os.path.join(out_dir,'ics209-plus-wf_sitreps_{}.csv'.format(final_timespan)),low_memory=False)
    #wfdf = wfdf.loc[:, ~wfdf.columns.str.contains('^Unnamed')]
    # create the incident level summary
    inc_df = _create_incident_summary(wfdf)
    # Save incident temp incident summary
    inc_df.to_csv(os.path.join(tmp_dir,'create-incident-summary-out.csv'))
    
    #inc_df = pd.read_csv(os.path.join(tmp_dir,'create-incident-summary-out.csv'),low_memory=False)
    inc_df = inc_df.loc[:, ~inc_df.columns.str.contains('^Unnamed')]
    inc_fod,cpx_fod = _join_with_fod_database(inc_df)
    inc_fired = _join_with_fired_database(inc_fod)
    
    # Save final output
    inc_fired.to_csv(os.path.join(out_dir,'ics209-plus-wf_incidents_{}.csv'.format(final_timespan)))
    cpx_fod.to_csv(os.path.join(out_dir,'ics209-plus-wf_complex_associations_{}.csv'.format(final_timespan)))
    wfdf = final_drop_extra_columns(wfdf)
    wfdf.to_csv(os.path.join(out_dir,'ics209-plus-wf_sitreps_{}.csv'.format(final_timespan)))
    
    

    