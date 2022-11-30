import os
import pandas as pd
import numpy as np
import ics209util
import earthpy as et

curr_timespan = '2014to2020'
data_dir = os.path.join(et.io.HOME, 'data')
out_dir = os.path.join(data_dir, 'out')
nwcgdata_dir = os.path.join(data_dir,'raw','common_data')
tmp_dir = os.path.join(data_dir, 'tmp')

def _record_delete_and_merge(df):
    
    # make sure REPORT_TO_DATE set to datetime
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE, errors='coerce')
    df = df.copy()
    
    # get merge/purge/delete specifications and make a copy
    df_cln = pd.read_csv(os.path.join(data_dir,
                                      'raw',
                                      'inc_clean',
                                      'ics-inc-cleanup{}.csv'.format(curr_timespan)))
    df_cln = df_cln[['INC_IDENTIFIER','action','ID']].copy()
    
    # filter for delete/merge/purge
    df_cln = df_cln[df_cln.action.isin(['delete','merge','rptdel'])]
    
    # get incident and report deletion lists
    inc_del_list = df_cln.loc[df_cln.action == 'delete']['INC_IDENTIFIER'].tolist()
    sit_del_list = df_cln.loc[df_cln.action == 'rptdel']['ID'].astype(int).tolist()
    
    # create merge dictionary
    merge = df_cln.loc[df_cln.action == 'merge'][['INC_IDENTIFIER','ID']].copy()
    merge_dict = merge.set_index('INC_IDENTIFIER').to_dict()['ID']
    #print(merge_dict)
    
    print("Before removing sitreps:")
    print(df.shape[0])
    
    # Save deleted sitreps
    df_del = df.loc[df.INC_IDENTIFIER.isin(inc_del_list)]
    df_del.to_csv(os.path.join(out_dir,"ics209_sitreps_deleted_curr_{}.csv".format(curr_timespan)))
    print("Records deleted: {}".format(df_del.shape[0]))
        
    # delete incidents and sitreps
    df = df.loc[~df.INC_IDENTIFIER.isin(inc_del_list)]
    df = df.loc[~df.INC209R_IDENTIFIER.isin(sit_del_list)]
    print("after:")
    print(df.shape[0])
    
    # merge incidents
    df.loc[df.INC_IDENTIFIER.isin([*merge_dict]),'INC_IDENTIFIER_OLD'] = df.INC_IDENTIFIER
    df = df.replace({"INC_IDENTIFIER": merge_dict})
    
    return df

def _general_field_cleaning(df):
    df = df.drop(df[(df.INC209R_IDENTIFIER == 427117)].index) #Test Complex
    
    df['COMPLEXITY_LEVEL_NARR'] = df.COMPLEXITY_LEVEL_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['HAZARDS_MATLS_INVOLVMENT_NARR'] = df.HAZARDS_MATLS_INVOLVMENT_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['LIFE_SAFETY_HEALTH_STATUS_NARR'] = \
                            df.LIFE_SAFETY_HEALTH_STATUS_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['PLANNED_ACTIONS'] = df.PLANNED_ACTIONS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['REMARKS'] = df.REMARKS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['SIGNIF_EVENTS_SUMMARY'] = df.SIGNIF_EVENTS_SUMMARY.apply(np.vectorize(ics209util.clean_narrative_text))
    df['STRATEGIC_DISCUSSION'] = df.STRATEGIC_DISCUSSION.apply(np.vectorize(ics209util.clean_narrative_text))
    df['STRATEGIC_OBJECTIVES'] = df.STRATEGIC_OBJECTIVES.apply(np.vectorize(ics209util.clean_narrative_text))
    df['WEATHER_CONCERNS_NARR'] = df.WEATHER_CONCERNS_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['UNIT_OR_OTHER_NARR'] = df.UNIT_OR_OTHER_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['CURRENT_THREAT_12'] = df.CURRENT_THREAT_12.apply(np.vectorize(ics209util.clean_narrative_text))
    df['PROJECTED_ACTIVITY_12'] = df.PROJECTED_ACTIVITY_12.apply(np.vectorize(ics209util.clean_narrative_text))
    
    return df
    
def _clean_and_format_date_and_time_fields(df):
    df.loc[df['INC209R_IDENTIFIER'] == 277716,'REPORT_TO_DATE'] = '2014-02-18 18:30:00' #was 2024
    df.loc[df['INC209R_IDENTIFIER'] == 321088,'DISCOVERY_DATE'] = '2014-03-22 21:30:00' #was 2011
    df.loc[df.REPORT_TO_DATE.isnull(),'REPORT_TO_DATE'] = df['REPORT_FROM_DATE'] # default report to if blank
    
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE)
    df['DISCOVERY_DATE'] = pd.to_datetime(df.DISCOVERY_DATE)
    df['REPORT_FROM_DATE'] = pd.to_datetime(df.REPORT_FROM_DATE)
    df['ANTICIPATED_COMPLETION_DATE'] = pd.to_datetime(df.ANTICIPATED_COMPLETION_DATE)
    
    df['CY'] = df.REPORT_FROM_DATE.dt.year
    df['START_YEAR'] = df.DISCOVERY_DATE.dt.year
    df['END_YEAR'] = df.ANTICIPATED_COMPLETION_DATE.dt.year
    df.loc[df.START_YEAR.isnull(), 'START_YEAR'] = df.CY
    
    return df
    
def _standardized_field_cleaning(df,lu_df):
    print("Cleaning fields...")
    lu_df = lu_df.copy()
    lu_df.drop_duplicates(inplace=True)
    
    # cause lookup
    ca_rows = lu_df[lu_df.CODE_TYPE == 'CA']
    ca_lu = ca_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    ca_lu.columns = ['CAUSE_IDENTIFIER','CAUSE']
    df = df.merge(ca_lu, how='left')
    
    # fuel model lookup
    fm_rows = lu_df[lu_df.CODE_TYPE == 'MATERIAL_INVOLVEMENT_TYPE']
    fm_lu = fm_rows[['LUCODES_IDENTIFIER','CODE_NAME']].copy()
    fm_lu.drop_duplicates(inplace=True)
    fm_lu.columns = ['FUEL_MODEL_IDENTIFIER','FUEL_MODEL']
    df = df.merge(fm_lu, how='left')
    
    fm_lu.columns = ['ADDNTL_FUEL_MODEL_IDENTIFIER','ADDNTL_FUEL_MODEL']
    df = df.merge(fm_lu, how='left')
    
    fm_lu.columns = ['SECNDRY_FUEL_MODEL_IDENTIFIER','SECNDRY_FUEL_MODEL']
    df = df.merge(fm_lu, how='left')
    
    # AREA MEASUREMENTS & Conversion to Acres:
    area_uom_rows = lu_df[lu_df.CODE_TYPE == 'AREA_UOM']
    area_uom_lu = area_uom_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    # AREA_MEASUREMENT
    area_uom_lu.columns = ['CURR_INC_AREA_UOM_IDENTIFIER','CURR_INC_AREA_UOM']
    df = df.merge(area_uom_lu, how='left')

    #PROJ_AREA_MEASUREMENT
    area_uom_lu.columns = ['PROJ_INC_AREA_UOM_IDENTIFIER','PROJ_INC_AREA_UOM']
    df = df.merge(area_uom_lu, how='left')
    
    # convert to 'ACRES'
    df.loc[df['CURR_INC_AREA_UOM'] == 'Acres','ACRES'] = df['CURR_INCIDENT_AREA']
    df.loc[df['CURR_INC_AREA_UOM'] == 'Square Miles','ACRES'] = df['CURR_INCIDENT_AREA'] * 640
    
    # team type
    tt_rows = lu_df[lu_df.CODE_TYPE == 'TT']
    tt_lu = tt_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    tt_lu.columns = ['INC_MGMT_ORG_IDENTIFIER','IMT_MGMT_ORG_DESC'] # column name matches historical field
    df = df.merge(tt_lu, how='left')
    
    # time zone
    tz_rows = lu_df[lu_df.CODE_TYPE == 'WORLD_TIME_ZONE']
    tz_lu = tz_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    tz_lu.columns = ['LOCAL_TIMEZONE_IDENTIFIER','LOCAL_TIMEZONE']
    df = df.merge(tz_lu, how='left')
    
    # principal meridian
    pm_rows = lu_df[lu_df.CODE_TYPE == 'PRINCIPAL_MERIDIAN']
    pm_lu = pm_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    pm_lu.columns = ['POO_LD_PM_IDENTIFIER','POO_LD_PM']
    df = df.merge(pm_lu, how='left')
    
    # state code/state name
    st_df = pd.read_csv(os.path.join(out_dir,
                                     'COMMONDATA_STATES_2014.csv'))
    st_df['STATE_CODE'] = st_df.STATE_CODE.apply(pd.to_numeric, args=('coerce',))
    st_lu = st_df[['STATE_CODE','STATE','STATE_NAME']]
    st_lu = st_lu.dropna(axis=0,how='any')
    st_lu.columns = ['POO_STATE_CODE','POO_STATE','POO_STATE_NAME']
    df = df.merge(st_lu, how='left')
    
    sc = {'C': True, 'S': False}
    df['COMPLEX'] = False
    df.COMPLEX = df.SINGLE_COMPLEX_FLAG.map(sc, na_action=None)
    
    # general fire behavior
    gfb_rows = lu_df[lu_df.CODE_TYPE == 'GENERAL_FIRE_BEHAVIOR']
    gfb_lu = gfb_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    gfb_lu.columns = ['GEN_FIRE_BEHAVIOR_IDENTIFIER','GEN_FIRE_BEHAVIOR']
    df = df.merge(gfb_lu, how='left')
    
    # fire behavior 1, 2, 3
    fb_rows = lu_df[lu_df.CODE_TYPE == 'FIRE_BEHAVIOR_CHARACTERISTIC']
    fb_lu = fb_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    fb_lu.columns = ['FIRE_BEHAVIOR_1_IDENTIFIER','FIRE_BEHAVIOR_1']
    df = df.merge(fb_lu, how='left')
    fb_lu.columns = ['FIRE_BEHAVIOR_2_IDENTIFIER','FIRE_BEHAVIOR_2']
    df = df.merge(fb_lu, how='left')
    fb_lu.columns = ['FIRE_BEHAVIOR_3_IDENTIFIER','FIRE_BEHAVIOR_3']
    df = df.merge(fb_lu, how='left')
    
    return df

def _derive_new_fields(df):
    df['CURRENT_THREAT_NARR'] = ics209util.combine_text_fields(df,'CURRENT_THREAT_12','CURRENT_THREAT_24','CURRENT_THREAT_48',
                                               'CURRENT_THREAT_72','CURRENT_THREAT_GT72')
    df['CRIT_RES_NEEDS_NARR'] = ics209util.combine_text_fields(df,'CRIT_RES_NEEDS_12','CRIT_RES_NEEDS_24','CRIT_RES_NEEDS_48',
                                                  'CRIT_RES_NEEDS_72','CRIT_RES_NEEDS_GT72')
    df['PROJECTED_ACTIVITY_NARR'] = ics209util.combine_text_fields(df,'PROJECTED_ACTIVITY_12','PROJECTED_ACTIVITY_24',
                                                'PROJECTED_ACTIVITY_48','PROJECTED_ACTIVITY_72','PROJECTED_ACTIVITY_GT72')
    df['STRATEGIC_NARR'] = ics209util.combine_text_fields(df,'STRATEGIC_DISCUSSION','STRATEGIC_OBJECTIVES')
    df['FIRE_EVENT_ID'] = df.INC_IDENTIFIER
    
    return df

def _patch_missing_sitrep_fields(df):
    #print("shape Before patch shape: {}".format(df.shape))
    inc_df = pd.read_csv(os.path.join(out_dir,
                                      'SIT209_HISTORY_INCIDENTS_{}.csv'.format(curr_timespan)),
                                      low_memory=False)
    
    # Adding in the IRWIN_IDENTIFIER, FIRECODE, PROT_UNIT here 4/11/22
    inc_xref = inc_df[['INCIDENT_IDENTIFIER','INCIDENT_NAME','INCIDENT_NUMBER','CAUSE_IDENTIFIER','DISCOVERY_DATE',\
                       'INCTYP_IDENTIFIER','POO_SHORT_LOCATION_DESC','POO_CITY','POO_STATE_CODE','POO_COUNTY_CODE',\
                       'POO_DONWCGU_OWN_IDENTIFIER','POO_LATITUDE','POO_LONGITUDE','POO_LD_PM_IDENTIFIER','POO_LD_QTR_QTR_SEC',\
                      'POO_LD_QTR_SEC','POO_LD_RGE','POO_LD_SEC','POO_LD_TWP','POO_US_NGR_XCOORD','POO_US_NGR_YCOORD',\
                      'POO_US_NGR_ZONE','POO_UTM_EASTING','POO_UTM_NORTHING','POO_UTM_ZONE','SINGLE_COMPLEX_FLAG',
                      'IRWIN_IDENTIFIER','FIRECODE','NWCG_PROT_UNIT_IDENTIFIER']].copy()
    inc_xref = inc_xref.groupby(['INCIDENT_IDENTIFIER']).first().reset_index()
    inc_xref.columns = ['INC_IDENTIFIER','M_INCIDENT_NAME','M_INCIDENT_NUMBER','M_CAUSE','M_DISC','M_INCTYP','M_LOC_DESC',\
                        'M_CITY','M_STATE','M_COUNTY','M_DONWCGU','M_LATITUDE','M_LONGITUDE','M_LD_PM',
                        'M_LD_QTR_QTR_SEC','M_LD_QTR_SEC','M_LD_RGE','M_LD_SEC','M_LD_TWP','M_XCOORD','M_YCOORD','M_NGR_ZONE',\
                       'M_EASTING','M_NORTHING','M_UTM_ZONE','M_CPX_FLAG','IRWIN_ID','FIRECODE','NWCG_IDENTIFIER']
    
    df['INCIDENT_NAME'] = df.INCIDENT_NAME.astype(str).str.strip()
    df['INCIDENT_NUMBER'] = df.INCIDENT_NUMBER.astype(str).str.strip()
    df['POO_CITY'] = df.POO_CITY.astype(str).str.strip()
    df['POO_SHORT_LOCATION_DESC'] = df.POO_SHORT_LOCATION_DESC.astype(str).str.strip()
    
    # join in columns
    df = df.merge(inc_xref, on='INC_IDENTIFIER', how='left')
    df['M_INCIDENT_NAME'] = df.M_INCIDENT_NAME.astype(str).str.strip()
    df['M_INCIDENT_NUMBER'] = df.M_INCIDENT_NUMBER.astype(str).str.strip()
    df['M_CITY'] = df.M_CITY.astype(str).str.strip()
    df['M_LOC_DESC'] = df.M_LOC_DESC.astype(str).str.strip()
    #print("nulls before inc name: {}".format((df.loc[df.INCIDENT_NAME=="nan"].shape[0]/df.shape[0])))
    df.loc[df.INCIDENT_NAME=="nan", 'INCIDENT_NAME'] = df.M_INCIDENT_NAME
    #print("nulls after inc name: {}".format((df.loc[df.INCIDENT_NAME=="nan"].shape[0]/df.shape[0])))
    #print("nulls before inc num: {}".format((df.loc[df.INCIDENT_NUMBER=="nan"].shape[0]/df.shape[0])))
    df.loc[df.INCIDENT_NUMBER=="nan", 'INCIDENT_NUMBER'] = df.M_INCIDENT_NUMBER
    #print("nulls after inc num: {}".format((df.loc[df.INCIDENT_NUMBER=="nan"].shape[0]/df.shape[0])))
    #print("nulls before cause id: {}".format((df.loc[df.CAUSE_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.CAUSE_IDENTIFIER.isnull()].shape[0]
    df.loc[df.CAUSE_IDENTIFIER.isnull(),'CAUSE_IDENTIFIER'] = df.M_CAUSE
    #print("nulls after cause id: {}".format((df.loc[df.CAUSE_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.CAUSE_IDENTIFIER.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("nulls before discovery: {}".format((df.loc[df.DISCOVERY_DATE.isnull()].shape[0]/df.shape[0])))
    df.loc[df.DISCOVERY_DATE.isnull(),'DISCOVERY_DATE'] = df.M_DISC
    #print("nulls after discovery: {}".format((df.loc[df.DISCOVERY_DATE.isnull()].shape[0]/df.shape[0])))
    #print("nulls before inc typ: {}".format((df.loc[df.INCTYP_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    df.loc[df.INCTYP_IDENTIFIER.isnull(),'INCTYP_IDENTIFIER'] = df.M_INCTYP
    #print("nulls after inc typ: {}".format((df.loc[df.INCTYP_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    
    # Location variables
    t1 = df.loc[df.POO_SHORT_LOCATION_DESC=="nan"].shape[0]
    #print("loc desc before: {}".format((df.loc[df.POO_SHORT_LOCATION_DESC=="nan"].shape[0]/df.shape[0])))
    df.loc[df.POO_SHORT_LOCATION_DESC=="nan",'POO_SHORT_LOCATION_DESC'] = df.M_LOC_DESC
    t2 = df.loc[df.POO_SHORT_LOCATION_DESC=="nan"].shape[0]
    #print("change: {}".format(t1-t2))
    #print("loc desc after: {}".format((df.loc[df.POO_SHORT_LOCATION_DESC=="nan"].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_CITY=="nan"].shape[0]
    #print("t1={}".format(t1))
    #print("city before: {}".format((df.loc[df.POO_CITY=="nan"].shape[0]/df.shape[0])))
    df.loc[df.POO_CITY=="nan",'POO_CITY'] = df.M_CITY
    t2 = df.loc[df.POO_CITY=="nan"].shape[0]
    #print("t2={}".format(t2))
    #print("city after: {}".format((df.loc[df.POO_CITY=="nan"].shape[0]/df.shape[0])))
    #print("change: {}".format(t1-t2))
    #print("county before: {}".format((df.loc[df.POO_COUNTY_CODE.isnull()].shape[0]/df.shape[0])))
    df.loc[df.POO_COUNTY_CODE.isnull(),'POO_COUNTY_CODE'] = df.M_COUNTY
    #print("county after: {}".format((df.loc[df.POO_COUNTY_CODE.isnull()].shape[0]/df.shape[0])))
    #print("state before: {}".format((df.loc[df.POO_STATE_CODE.isnull()].shape[0]/df.shape[0])))
    df.loc[df.POO_STATE_CODE.isnull(),'POO_STATE_CODE'] = df.M_STATE
    #print("state after: {}".format((df.loc[df.POO_STATE_CODE.isnull()].shape[0]/df.shape[0])))
    #print("donwcgu before: {}".format((df.loc[df.POO_DONWCGU_OWN_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    df.loc[df.POO_DONWCGU_OWN_IDENTIFIER.isnull(),'POO_DONWCGU_OWN_IDENTIFIER'] = df.M_DONWCGU
    #print("donwcgu after: {}".format((df.loc[df.POO_DONWCGU_OWN_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    
    # Default latitude/longitude to value in INCIDENTS record for consistency
    #print("latitude before: {}".format((df.loc[df.POO_LATITUDE.isnull()].shape[0]/df.shape[0])))
    df.loc[~df.M_LATITUDE.isnull(),'POO_LATITUDE'] = df.M_LATITUDE
    #print("latitude after: {}".format((df.loc[df.POO_LATITUDE.isnull()].shape[0]/df.shape[0])))
    #print("longitude before: {}".format((df.loc[df.POO_LONGITUDE.isnull()].shape[0]/df.shape[0])))
    df.loc[~df.M_LONGITUDE.isnull(),'POO_LONGITUDE'] = df.M_LONGITUDE
    #print("longitude after: {}".format((df.loc[df.POO_LONGITUDE.isnull()].shape[0]/df.shape[0])))
    
    #print("ld pm before: {}".format((df.loc[df.POO_LD_PM_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_PM_IDENTIFIER.isnull()].shape[0]
    df.loc[df.POO_LD_PM_IDENTIFIER.isnull(),'POO_LD_PM_IDENTIFIER'] = df.M_LD_PM
    #print("ld pm after: {}".format((df.loc[df.POO_LD_PM_IDENTIFIER.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_PM_IDENTIFIER.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("qtr qtr sec before: {}".format((df.loc[df.POO_LD_QTR_QTR_SEC.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_QTR_QTR_SEC.isnull()].shape[0]
    df.loc[df.POO_LD_QTR_QTR_SEC.isnull(),'POO_LD_QTR_QTR_SEC'] = df.M_LD_QTR_QTR_SEC
    #print("qtr qtr sec after: {}".format((df.loc[df.POO_LD_QTR_QTR_SEC.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_QTR_QTR_SEC.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("qtr sec before: {}".format((df.loc[df.POO_LD_QTR_SEC.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_QTR_SEC.isnull()].shape[0]
    df.loc[df.POO_LD_QTR_SEC.isnull(),'POO_LD_QTR_SEC'] = df.M_LD_QTR_SEC
    #print("qtr sec after: {}".format((df.loc[df.POO_LD_QTR_SEC.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_QTR_SEC.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("rge before: {}".format((df.loc[df.POO_LD_RGE.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_RGE.isnull()].shape[0]
    df.loc[df.POO_LD_RGE.isnull(),'POO_LD_RGE'] = df.M_LD_RGE
    #print("rge after: {}".format((df.loc[df.POO_LD_RGE.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_RGE.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("sec before: {}".format((df.loc[df.POO_LD_SEC.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_SEC.isnull()].shape[0]
    df.loc[df.POO_LD_SEC.isnull(),'POO_LD_SEC'] = df.M_LD_SEC
    #print("sec after: {}".format((df.loc[df.POO_LD_SEC.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_SEC.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("twp before: {}".format((df.loc[df.POO_LD_TWP.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_LD_TWP.isnull()].shape[0]
    df.loc[df.POO_LD_TWP.isnull(),'POO_LD_TWP'] = df.M_LD_TWP
    #print("twp after: {}".format((df.loc[df.POO_LD_TWP.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_LD_TWP.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("xcoord before: {}".format((df.loc[df.POO_US_NGR_XCOORD.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_US_NGR_XCOORD.isnull()].shape[0]
    df.loc[df.POO_US_NGR_XCOORD.isnull(),'POO_US_NGR_XCOORD'] = df.M_XCOORD
    #print("xcoord after: {}".format((df.loc[df.POO_US_NGR_XCOORD.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_US_NGR_XCOORD.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("ycoord before: {}".format((df.loc[df.POO_US_NGR_YCOORD.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_US_NGR_YCOORD.isnull()].shape[0]
    df.loc[df.POO_US_NGR_YCOORD.isnull(),'POO_US_NGR_YCOORD'] = df.M_YCOORD
    #print("ycoord after: {}".format((df.loc[df.POO_US_NGR_YCOORD.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_US_NGR_YCOORD.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("ngr zone before: {}".format((df.loc[df.POO_US_NGR_ZONE.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_US_NGR_ZONE.isnull()].shape[0]
    df.loc[df.POO_US_NGR_ZONE.isnull(),'POO_US_NGR_ZONE'] = df.M_NGR_ZONE
    #print("ngr zone after: {}".format((df.loc[df.POO_US_NGR_ZONE.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_US_NGR_ZONE.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("utm easting before: {}".format((df.loc[df.POO_UTM_EASTING.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_UTM_EASTING.isnull()].shape[0]
    df.loc[df.POO_UTM_EASTING.isnull(),'POO_UTM_EASTING'] = df.M_EASTING
    #print("utm easting after: {}".format((df.loc[df.POO_UTM_EASTING.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_UTM_EASTING.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("utm northing before: {}".format((df.loc[df.POO_UTM_NORTHING.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_UTM_NORTHING.isnull()].shape[0]
    df.loc[df.POO_UTM_NORTHING.isnull(),'POO_UTM_NORTHING'] = df.M_NORTHING
    #print("utm northing after: {}".format((df.loc[df.POO_UTM_NORTHING.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_UTM_NORTHING.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("utm zone before: {}".format((df.loc[df.POO_UTM_ZONE.isnull()].shape[0]/df.shape[0])))
    t1 = df.loc[df.POO_UTM_ZONE.isnull()].shape[0]
    df.loc[df.POO_UTM_ZONE.isnull(),'POO_UTM_ZONE'] = df.M_UTM_ZONE
    #print("utm zone after: {}".format((df.loc[df.POO_UTM_ZONE.isnull()].shape[0]/df.shape[0])))
    t2 = df.loc[df.POO_UTM_ZONE.isnull()].shape[0]
    #print("change: {}".format(t1-t2))
    #print("single cpx flag: {}".format((df.loc[df.SINGLE_COMPLEX_FLAG.isnull()].shape[0]/df.shape[0])))
    df.loc[df.SINGLE_COMPLEX_FLAG.isnull(),'SINGLE_COMPLEX_FLAG'] = df.M_CPX_FLAG
    #print("single cpx after: {}".format((df.loc[df.SINGLE_COMPLEX_FLAG.isnull()].shape[0]/df.shape[0])))
    
    df.drop(['M_INCIDENT_NAME','M_INCIDENT_NUMBER','M_CAUSE','M_DISC','M_INCTYP','M_CITY','M_COUNTY',\
            'M_DONWCGU','M_LATITUDE','M_LONGITUDE','M_LD_PM','M_LD_QTR_QTR_SEC','M_LD_QTR_SEC','M_LD_RGE',\
            'M_LD_SEC','M_LD_TWP','M_LOC_DESC','M_STATE','M_XCOORD','M_YCOORD','M_NGR_ZONE','M_EASTING',\
            'M_NORTHING','M_UTM_ZONE','M_CPX_FLAG'],axis=1,inplace=True)
    print("shape After patch {}".format(df.shape))

    return df
    
def _create_incident_id(df):
    # forward fill added to fix sparcity issue 2015/2016 data when creating incident ID
    print("Creating incident ID")
    df = df.sort_values(['INC_IDENTIFIER','REPORT_TO_DATE']).copy()
    df = df.reset_index(drop=True)
    
    dfinc = df.sort_values(['INC_IDENTIFIER','REPORT_TO_DATE']).groupby('INC_IDENTIFIER').nth(-1).reset_index()
    # Set incident ID to final fire name for INC_IDENTIFIER
    # Use INC_IDENTIFIER, remove str.strip()? temporary? 1/11/2021 - Lise (save previous as INCIDENT_ID_OLD)
    dfinc['INCIDENT_ID'] = dfinc.START_YEAR.astype(int).astype(str) + '_' + dfinc.INC_IDENTIFIER.astype(int).astype(str) + \
                            '_' + dfinc.INCIDENT_NAME.astype(str).str.strip().str.upper()
    dfinc['INCIDENT_ID_OLD'] = dfinc.START_YEAR.astype(int).astype(str) + '_' + dfinc.INCIDENT_NUMBER.str.strip().astype(str) + \
                            '_' + dfinc.INCIDENT_NAME.astype(str).str.strip().str.upper()
    g1 = dfinc.groupby(['INCIDENT_ID']).size().reset_index(name="num_rows")
    duplicate_inc_ids = g1.loc[g1.num_rows>1]
    duplicate_inc_ids.to_csv('../../data/tmp/dup_inc_identfier.csv')
    print("#incidents: {}".format(g1.shape[0]))
    print("% duplicates = {}".format(duplicate_inc_ids.shape[0]/g1.shape[0]))
    print("Duplicate INC Incident Identifiers:") # no longer splitting on incident_number
    print(g1.loc[g1.num_rows>1])
    dfIDxref = dfinc[['INC_IDENTIFIER','INCIDENT_ID','INCIDENT_ID_OLD']]
    df = pd.merge(df, dfIDxref, on=['INC_IDENTIFIER'], how='left')
    return df

def _latitude_longitude_updates(df):
    curr_loc = pd.read_csv(os.path.join(data_dir,
                                        'raw',
                                        'latlong_clean',
                                        'curr_cleaned_ll-fod.csv'))
    curr_loc = curr_loc.loc[:, ~curr_loc.columns.str.contains('^Unnamed')]
    df = df.merge(curr_loc, on=['INC_IDENTIFIER'],how='left')
    # Set the Update Flag
    df.loc[df.lat_c.notnull(),'LL_UPDATE'] = True
    # Case #1: Update lat/long
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'POO_LATITUDE'] = df.lat_c # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'POO_LONGITUDE'] = df.long_c # set longitude to nan
    # Case #2: Unable to fix so set to null
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'POO_LATITUDE'] = np.nan # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'POO_LONGITUDE'] = np.nan # set longitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'LL_CONFIDENCE'] = 'N'
    return df

def _get_str_ext(lu_tbl):
    # read in structures table
    dfc_str = pd.read_csv(os.path.join(out_dir,
                                      'SIT209_HISTORY_INCIDENT_209_AFFECTED_STRUCTS_{}.csv'.format(curr_timespan)))
    dfc_str = dfc_str.loc[:, ~dfc_str.columns.str.contains('^Unnamed')]
    # Added to deal with duplication issue 8/16/21
    dfc_str = dfc_str.drop_duplicates()
    
    # get structure type from the lookup table
    sst_rows = lu_tbl[lu_tbl.CODE_TYPE == 'STRUCTURE_SUMMARY_TYPE']
    sst_lu = sst_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    sst_lu.columns = ['SST_IDENTIFIER','SST']
    sst_lu.shape

    dfc_str = dfc_str.merge(sst_lu, how='left')
    # Pivot the table
    dfc_str_piv = dfc_str.pivot_table(index=['INC209R_IDENTIFIER'], columns=['SST'],
                                             values=['QTY_DESTROYED','QTY_THREATENED_72','QTY_DAMAGED'],aggfunc=np.mean)
    # Clean up the column names
    dfc_str_piv.columns = ["_".join((i,j)) for i,j in dfc_str_piv.columns]
    dfc_str_piv = dfc_str_piv.fillna(0)
    dfc_str_piv['STR_DAMAGED'] = dfc_str_piv['QTY_DAMAGED_MC/R'] + dfc_str_piv.QTY_DAMAGED_MR + dfc_str_piv.QTY_DAMAGED_NRC +\
        dfc_str_piv.QTY_DESTROYED_OTH + dfc_str_piv.QTY_DESTROYED_SR
    dfc_str_piv['STR_DESTROYED'] = dfc_str_piv['QTY_DESTROYED_MC/R'] + dfc_str_piv.QTY_DESTROYED_MR +\
        dfc_str_piv.QTY_DESTROYED_NRC + dfc_str_piv.QTY_DESTROYED_OTH + dfc_str_piv.QTY_DESTROYED_SR
    dfc_str_piv['STR_THREATENED'] = dfc_str_piv['QTY_THREATENED_72_MC/R'] + dfc_str_piv.QTY_THREATENED_72_MR +\
        dfc_str_piv.QTY_THREATENED_72_NRC + dfc_str_piv.QTY_THREATENED_72_OTH + dfc_str_piv.QTY_THREATENED_72_SR
    dfc_str_piv['STR_DAMAGED_RES'] = dfc_str_piv['QTY_DAMAGED_MC/R'] + dfc_str_piv.QTY_DAMAGED_MR + dfc_str_piv.QTY_DAMAGED_SR
    dfc_str_piv['STR_DESTROYED_RES'] = dfc_str_piv['QTY_DESTROYED_MC/R'] + dfc_str_piv.QTY_DESTROYED_MR +\
        dfc_str_piv.QTY_DESTROYED_SR
    dfc_str_piv['STR_THREATENED_RES'] = dfc_str_piv['QTY_THREATENED_72_MC/R'] + dfc_str_piv.QTY_THREATENED_72_MR +\
        dfc_str_piv.QTY_THREATENED_72_SR
        
    dfc_str_piv.reset_index(inplace=True)
    dfc_str_merge = dfc_str_piv[['INC209R_IDENTIFIER','STR_DAMAGED','STR_DESTROYED','STR_THREATENED',
                                'STR_DAMAGED_RES','STR_DESTROYED_RES','STR_THREATENED_RES','QTY_DAMAGED_NRC',\
                                'QTY_DESTROYED_NRC','QTY_THREATENED_72_NRC']]
    dfc_str_merge.columns = dfc_str_merge.columns.str.replace('QTY_DAMAGED_NRC','STR_DAMAGED_COMM')
    dfc_str_merge.columns = dfc_str_merge.columns.str.replace('QTY_DESTROYED_NRC','STR_DESTROYED_COMM')
    dfc_str_merge.columns = dfc_str_merge.columns.str.replace('QTY_THREATENED_72_NRC','STR_THREATENED_COMM')
    return dfc_str_merge
        
def _get_res_ext(lu_tbl):
    dfc_res = pd.read_csv(os.path.join(out_dir,
                                       'SIT209_HISTORY_INCIDENT_209_RES_UTILIZATIONS_{}.csv'.format(curr_timespan)))
    dfc_res = dfc_res.loc[:, ~dfc_res.columns.str.contains('^Unnamed')]
    print(dfc_res.shape)
    dfc_res.drop_duplicates(inplace=True)
    print(dfc_res.shape)
    
    res_rows = lu_tbl[lu_tbl.CODE_TYPE == 'RESOURCE_TYPE']
    res_lu = res_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    res_lu.columns = ['RESTYP_IDENTIFIER','RESTYP']

    dfc_res = dfc_res.merge(res_lu, how='left')
    dfc_res.to_csv(os.path.join(out_dir,
                                'SIT209_HISTORY_INCIDENT_209_RES_UTILIZATIONS_{}.csv'.format(curr_timespan)))
    
    # pivot the table
    dfc_res_piv = dfc_res.pivot_table(index=['INC209R_IDENTIFIER'], columns=['RESTYP'],
                                         values=['RESOURCE_QUANTITY','RESOURCE_PERSONNEL'],aggfunc=np.mean)
    dfc_res_piv.columns = ["_".join((i,j)) for i,j in dfc_res_piv.columns]
    dfc_res_piv.reset_index(inplace=True)
    dfc_res_piv = dfc_res_piv.fillna(0)
    
    dfc_res_piv['TOTAL_PERSONNEL'] = dfc_res_piv.RESOURCE_PERSONNEL_AMBL + dfc_res_piv.RESOURCE_PERSONNEL_ASM +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_AT1 + dfc_res_piv.RESOURCE_PERSONNEL_AT2 +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_AT3 + dfc_res_piv.RESOURCE_PERSONNEL_BUS +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_CFR + dfc_res_piv.RESOURCE_PERSONNEL_CR1 +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_CR2 + dfc_res_piv.RESOURCE_PERSONNEL_CR2IA +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_CRC + dfc_res_piv.RESOURCE_PERSONNEL_DECON +\
                                     dfc_res_piv.RESOURCE_PERSONNEL_DOZR + dfc_res_piv.RESOURCE_PERSONNEL_ENG1 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_ENG2 + dfc_res_piv.RESOURCE_PERSONNEL_ENG3 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_ENG4 + dfc_res_piv.RESOURCE_PERSONNEL_ENG5 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_ENG6 + dfc_res_piv.RESOURCE_PERSONNEL_ENG7 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_FB + dfc_res_piv.RESOURCE_PERSONNEL_FDU + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_FWAT + dfc_res_piv.RESOURCE_PERSONNEL_FWRE + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_GISU + dfc_res_piv.RESOURCE_PERSONNEL_HAZTR + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_HAZU + dfc_res_piv.RESOURCE_PERSONNEL_HEL1 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_HEL2 + dfc_res_piv.RESOURCE_PERSONNEL_HEL3 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_ILAU + dfc_res_piv.RESOURCE_PERSONNEL_K9SAR + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_LEAD + dfc_res_piv.RESOURCE_PERSONNEL_MAST + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_MCC + dfc_res_piv.RESOURCE_PERSONNEL_MKU + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_MSAR + dfc_res_piv.RESOURCE_PERSONNEL_MSHWR + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_MSQD + dfc_res_piv.RESOURCE_PERSONNEL_OVH + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_QUINT + dfc_res_piv.RESOURCE_PERSONNEL_RB + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_RMDU + dfc_res_piv.RESOURCE_PERSONNEL_SALU + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_SEAT + dfc_res_piv.RESOURCE_PERSONNEL_SKID + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_TB + dfc_res_piv.RESOURCE_PERSONNEL_TPL1 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_TPL2 + dfc_res_piv.RESOURCE_PERSONNEL_TPL3 + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_TPL4 + dfc_res_piv.RESOURCE_PERSONNEL_TRT + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_TRVH + dfc_res_piv.RESOURCE_PERSONNEL_USAR + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_USRT + dfc_res_piv.RESOURCE_PERSONNEL_VLAT + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_WPP + dfc_res_piv.RESOURCE_PERSONNEL_WRT + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_WTDP + dfc_res_piv.RESOURCE_PERSONNEL_WTDS + \
                                     dfc_res_piv.RESOURCE_PERSONNEL_WTDT
                                    
    dfc_res_piv['TOTAL_AERIAL'] = dfc_res_piv.RESOURCE_QUANTITY_ASM + dfc_res_piv.RESOURCE_QUANTITY_AT1 + \
                                  dfc_res_piv.RESOURCE_QUANTITY_AT2 + dfc_res_piv.RESOURCE_QUANTITY_AT3 + \
                                  dfc_res_piv.RESOURCE_QUANTITY_CFR + dfc_res_piv.RESOURCE_QUANTITY_FWAT + \
                                  dfc_res_piv.RESOURCE_QUANTITY_FWRE + dfc_res_piv.RESOURCE_QUANTITY_HEL1 + \
                                  dfc_res_piv.RESOURCE_QUANTITY_HEL2 + dfc_res_piv.RESOURCE_QUANTITY_HEL3 + \
                                  dfc_res_piv.RESOURCE_QUANTITY_LEAD + dfc_res_piv.RESOURCE_QUANTITY_SEAT + \
                                  dfc_res_piv.RESOURCE_QUANTITY_VLAT

    # save output files
    dfc_pers = dfc_res_piv[['INC209R_IDENTIFIER','TOTAL_PERSONNEL','TOTAL_AERIAL']]
    return dfc_pers
    
def _get_curr_cslty_ext(lu_tbl):
    # Open casualty/illness table
    dfc_cslty = pd.read_csv(os.path.join(out_dir,
                                         'SIT209_HISTORY_INCIDENT_209_CSLTY_ILLNESSES_{}.csv'.format(curr_timespan)))
    dfc_cslty = dfc_cslty.loc[:, ~dfc_cslty.columns.str.contains('^Unnamed')]
    
    print(dfc_cslty.shape)
    dfc_cslty.drop_duplicates(inplace=True)
    print(dfc_cslty.shape)
    
    r_df = dfc_cslty.loc[dfc_cslty.RESPONDER_PUBLIC_FLAG == "R"]
    print(r_df.shape)
    p_df = dfc_cslty.loc[dfc_cslty.RESPONDER_PUBLIC_FLAG == "P"]
    print(p_df.shape)

    # Get casualty/illness values
    cit_rows = lu_tbl[lu_tbl.CODE_TYPE == 'CASUALTY_ILLNESS_TYPE']
    cit_lu = cit_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    cit_lu.columns = ['CIT_IDENTIFIER','CIT']
    
    r_df = r_df.merge(cit_lu, on='CIT_IDENTIFIER', how='left')
    p_df = p_df.merge(cit_lu, on='CIT_IDENTIFIER', how='left')
    
    # Get Public
    p_df = p_df.loc[p_df.CIT.isin(['II','E','F'])] # restrict to Injury/Illness, Evacuation, Fatality
    
    # Sum public/responder to simplify pivot
    pdf_qtyrep = p_df.groupby(['INC209R_IDENTIFIER','CIT']).QTY_THIS_REP_PERIOD.sum().reset_index(name="RPT_P")
    pdf_todate = p_df.groupby(['INC209R_IDENTIFIER','CIT']).QTY_TO_DATE.sum().reset_index(name="TOT_P")
    pdf = pdf_qtyrep.merge(pdf_todate,on=['INC209R_IDENTIFIER','CIT'])
    
    # pivot table and rename columns
    pdf_piv = pdf.pivot_table(index=['INC209R_IDENTIFIER'],columns=['CIT'],values=['RPT_P','TOT_P'])
    pdf_piv.columns = ["_".join((i,j)) for i,j in pdf_piv.columns]
    pdf_piv.reset_index(inplace=True)
    pdf_piv.fillna(0, inplace=True)
    
    # Get Responder
    r_df = r_df.loc[r_df.CIT.isin(['II','F'])] # restrict to Injury/Illness & Fatality
    
    # Sum public/responder to simplify pivot
    rdf_qtyrep = r_df.groupby(['INC209R_IDENTIFIER','CIT']).QTY_THIS_REP_PERIOD.sum().reset_index(name="RPT_R")
    rdf_todate = r_df.groupby(['INC209R_IDENTIFIER','CIT']).QTY_TO_DATE.sum().reset_index(name="TOT_R")
    rdf = rdf_qtyrep.merge(rdf_todate,on=['INC209R_IDENTIFIER','CIT'])
    
    # pivot table and rename columns
    rdf_piv = rdf.pivot_table(index=['INC209R_IDENTIFIER'],columns=['CIT'],values=['RPT_R','TOT_R'])
    rdf_piv.columns = ["_".join((i,j)) for i,j in rdf_piv.columns]
    rdf_piv.reset_index(inplace=True)
    rdf_piv.fillna(0, inplace=True)
    
    df_cslty_piv = rdf_piv.merge(pdf_piv,on='INC209R_IDENTIFIER',how='outer')
    df_cslty_piv.fillna(0,inplace=True)
    
    df_cslty_piv.columns = ['INC209R_IDENTIFIER',
                       'RPT_R_FATALITIES','RPT_R_INJURIES',
                       'TOTAL_R_FATALITIES','TOTAL_R_INJURIES',
                       'RPT_EVACUATIONS','RPT_P_FATALITIES','RPT_P_INJURIES',
                       'TOTAL_EVACUATIONS','TOTAL_P_FATALITIES','TOTAL_P_INJURIES']
    df_cslty_piv['RPT_FATALITIES'] = df_cslty_piv.RPT_R_FATALITIES + df_cslty_piv.RPT_P_FATALITIES
    df_cslty_piv['FATALITIES'] = df_cslty_piv.TOTAL_R_FATALITIES + df_cslty_piv.TOTAL_P_FATALITIES
    df_cslty_piv['INJURIES'] = df_cslty_piv.RPT_R_INJURIES + df_cslty_piv.RPT_P_INJURIES
    df_cslty_piv['INJURIES_TO_DATE'] = df_cslty_piv.TOTAL_R_INJURIES + df_cslty_piv.TOTAL_P_INJURIES
    df_cslty_piv['NUM_EVACUATED'] = df_cslty_piv.RPT_EVACUATIONS
    df_cslty_piv['NUM_EVAC_TO_DATE'] = df_cslty_piv.RPT_EVACUATIONS
    
    return df_cslty_piv


def _get_curr_sup_ext(lu_tbl):
    dfc_sup = pd.read_csv(os.path.join(out_dir,
                                      'SIT209_HISTORY_INCIDENT_209_STRATEGIES_{}.csv'.format(curr_timespan)))
    dfc_sup = dfc_sup.loc[:, ~dfc_sup.columns.str.contains('^Unnamed')]
    # Added to deal with duplication issue 8/16/21
    dfc_sup.drop_duplicates(inplace=True)
    
    # get structure type from the lookup table
    sup_rows = lu_tbl[lu_tbl.CODE_TYPE == 'FIRE_SUPPRESSION_STRATEGY']
    sup_lu = sup_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    sup_lu.columns = ['STRATEGY_IDENTIFIER','STRATEGY']
    
    # Merge in suppression method description
    dfc_sup = dfc_sup.merge(sup_lu, how='left')
    # Pivot the table
    dfc_sup_piv = dfc_sup.pivot_table(index=['INC209R_IDENTIFIER'], columns=['STRATEGY'], values=['PERCENT_UTILIZED'])
    dfc_sup_piv.columns = ["_".join((i,j)) for i,j in dfc_sup_piv.columns]
    dfc_sup_piv.reset_index(inplace=True)
    # Fill nulls
    dfc_sup_piv = dfc_sup_piv.fillna(0)
    
    dfc_sup_piv.columns = ['INC209R_IDENTIFIER','PERCENT_C','PERCENT_FS','PERCENT_M','PERCENT_PZP']
    
    # Create SUPPRESSION_METHOD, SUPPRESSION_METHOD_FULLNAME fields
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_PZP == 100, 'SUPPRESSION_METHOD'] = 'PZP'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_PZP == 100, 'SUPPRESSION_METHOD_FULLNAME'] = 'Point Zone Protection'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_C == 100, 'SUPPRESSION_METHOD'] = 'C'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_C == 100, 'SUPPRESSION_METHOD_FULLNAME'] = 'Confine'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_M == 100, 'SUPPRESSION_METHOD'] = 'M'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_M == 100, 'SUPPRESSION_METHOD_FULLNAME'] = 'Monitor'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_FS == 100, 'SUPPRESSION_METHOD'] = 'FS'
    dfc_sup_piv.loc[dfc_sup_piv.PERCENT_FS == 100, 'SUPPRESSION_METHOD_FULLNAME'] = 'Full Supression'
    
    dfc_sup_piv['TOTAL'] = dfc_sup_piv.PERCENT_C + dfc_sup_piv.PERCENT_FS + \
                           dfc_sup_piv.PERCENT_M + dfc_sup_piv.PERCENT_PZP
    
    dfc_sup_piv.loc[(dfc_sup_piv.SUPPRESSION_METHOD.isnull()) & (dfc_sup_piv.TOTAL > 0), 
                                                                'SUPPRESSION_METHOD'] = 'MMS'
    dfc_sup_piv.loc[dfc_sup_piv.SUPPRESSION_METHOD == "MMS", 'SUPPRESSION_METHOD_FULLNAME'] = \
                                                                'Managed with Multiple Strategies '
    
    dfc_sup_piv.drop(['TOTAL'],axis=1,inplace=True)
    return dfc_sup_piv

def clean_nwcg_commondata(nwcg_tbl):
    data_df = nwcg_tbl.copy()
    
    #create_update dictionary
    upd_df = pd.read_csv(os.path.join(nwcgdata_dir,'nwcg-prot-unit-fix.csv'))
    upd_tbl = upd_df.loc[upd_df.CORRECT == "Y"].copy()
    upd_tbl = upd_tbl[['NWCG_IDENTIFIER','PROT_UNIT_NAME']]
    upd_tbl['PROT_UNIT_NAME'] = upd_tbl.PROT_UNIT_NAME.str.strip()
    upd_dict = dict(zip(upd_tbl.NWCG_IDENTIFIER, upd_tbl.PROT_UNIT_NAME))
    
    # strip text fields and apply the update
    data_df['PROT_UNIT_NAME'] = data_df.PROT_UNIT_NAME.str.strip()
    nwcg_tbl['PROT_UNIT_TYPE'] = nwcg_tbl.PROT_UNIT_TYPE.str.strip()
    data_df.loc[data_df.NWCG_IDENTIFIER.isin(list(upd_dict.keys())),'PROT_UNIT_NAME'] = \
    data_df['NWCG_IDENTIFIER'].map(upd_dict,na_action=None)
    
    data_df['PROT_UNIT_ID'] = data_df.PROT_UNIT_ID.str.strip()
    # drop the duplicates
    data_df.drop_duplicates(inplace=True)
    
    # return a clean commondata table
    return data_df

def _link_prot_unit(df):
    
    # Read in commondata nwcg units file
    nwcg_units = pd.read_csv(os.path.join(out_dir,'COMMONDATA_NWCG_UNITS_{}.csv'.format(curr_timespan)),
                            low_memory=False)
    nwcg_tbl = nwcg_units[['NWCG_IDENTIFIER','UNITID','NAME','UNIT_TYPE']].copy()
    nwcg_tbl.columns = ['NWCG_IDENTIFIER', 'PROT_UNIT_ID','PROT_UNIT_NAME','PROT_UNIT_TYPE']
    nwcg_tbl = clean_nwcg_commondata(nwcg_tbl)
    
    df = df.merge(nwcg_tbl, on=['NWCG_IDENTIFIER'],how='left') 
    return df

def current_merge_prep():
    df = pd.read_csv(os.path.join(out_dir,
                                  'SIT209_HISTORY_INCIDENT_209_REPORTS_{}.csv'.format(curr_timespan)), 
                     parse_dates=True,
                     low_memory=False)
    lu_df = pd.read_csv(os.path.join(out_dir,
                                     'SIT209_LOOKUP_CODES.csv'),low_memory=False)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    print(df.shape)
    df.drop_duplicates(inplace=True)
    print(df.shape)
    
    # Initial cleaning steps
    df = _record_delete_and_merge(df)
    df = _patch_missing_sitrep_fields(df)
    df = _clean_and_format_date_and_time_fields(df)
    df = _derive_new_fields(df)
    df = _general_field_cleaning(df)
    df = _standardized_field_cleaning(df,lu_df)
    df = _create_incident_id(df)
    df = _latitude_longitude_updates(df)
    df = _link_prot_unit(df) # added for 2.0
    
    
    # save id xref and output files
    dfIDxref = df[['INC_IDENTIFIER','INCIDENT_ID']]
    dfIDxref = dfIDxref.drop_duplicates()
    df_str_ext = _get_str_ext(lu_df)
    df_str_ext.to_csv(os.path.join(tmp_dir,'str_out.csv'))
    df_ext = pd.merge(df,df_str_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After structure merge: {}".format(df_ext.shape))
    df_res_ext = _get_res_ext(lu_df)
    df_ext = pd.merge(df_ext,df_res_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After resource merge: {}".format(df_ext.shape))
    df_cslty_ext = _get_curr_cslty_ext(lu_df)
    df_ext = pd.merge(df_ext,df_cslty_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After casualty merge: {}".format(df_ext.shape))
    df_sup_ext = _get_curr_sup_ext(lu_df)
    df_ext = pd.merge(df_ext,df_sup_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After suppression strategy merge: {}".format(df_ext.shape))
    print("Current System merge preparation complete. {}".format(df_ext.shape))
    df_ext.to_csv(os.path.join(out_dir,
                               'SIT209_HISTORY_INCIDENT_209_REPORTS_{}_cleaned.csv'.format(curr_timespan)))

