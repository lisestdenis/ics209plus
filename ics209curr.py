import pandas as pd
import numpy as np
import ics209util

curr_timespan = '2014'

def _general_field_cleaning(df):
    df = df.drop(df[(df.INC209R_IDENTIFIER == 427117)].index) #Test Complex
    
    df['COMPLEXITY_LEVEL_NARR'] = df.COMPLEXITY_LEVEL_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['HAZARDS_MATLS_INVOLVMENT_NARR'] = df.HAZARDS_MATLS_INVOLVMENT_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['LIFE_SAFETY_HEALTH_STATUS_NARR'] = \
                            df.LIFE_SAFETY_HEALTH_STATUS_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['PLANNED_ACTIONS'] = df.PLANNED_ACTIONS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['REMARKS'] = df.REMARKS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['SIGNIF_EVENTS_SUMMARY'] = df.SIGNIF_EVENTS_SUMMARY.apply(np.vectorize(ics209util.clean_narrative_text))
    df['SRATEGIC_DISCUSSION'] = df.STRATEGIC_DISCUSSION.apply(np.vectorize(ics209util.clean_narrative_text))
    df['SRATEGIC_OBJECTIVES'] = df.STRATEGIC_OBJECTIVES.apply(np.vectorize(ics209util.clean_narrative_text))
    df['WEATHER_CONCERNS_NARR'] = df.WEATHER_CONCERNS_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['UNIT_OR_OTHER_NARR'] = df.UNIT_OR_OTHER_NARR.apply(np.vectorize(ics209util.clean_narrative_text))
    df['CURRENT_THREAT_12'] = df.CURRENT_THREAT_12.apply(np.vectorize(ics209util.clean_narrative_text))
    df['PROJECTED_ACTIVITY_12'] = df.PROJECTED_ACTIVITY_12.apply(np.vectorize(ics209util.clean_narrative_text))
    
    return df
    
def _clean_and_format_date_and_time_fields(df):
    df.loc[df['INC209R_IDENTIFIER'] == 277716,'REPORT_TO_DATE'] = '2014-02-18 18:30:00' #was 2024
    df.loc[df['INC209R_IDENTIFIER'] == 321088,'DISCOVERY_DATE'] = '2014-03-22 21:30:00' #was 2011
    df.loc[df.REPORT_TO_DATE.isnull(),'REPORT_TO_DATE'] = df['REPORT_FROM_DATE'] # default report to if blank
    
    df['CY'] = df.REPORT_FROM_DATE.str[:4]
    df['START_YEAR'] = df.DISCOVERY_DATE.str[:4]
    
    return df
    
def _standardized_field_cleaning(df,lu_df):
    print("Cleaning fields...")
    # cause lookup
    ca_rows = lu_df[lu_df.CODE_TYPE == 'CA']
    ca_lu = ca_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    ca_lu.columns = ['CAUSE_IDENTIFIER','CAUSE']
    df = df.merge(ca_lu, how='left')
    print(df.shape)
    
    # fuel model lookup
    fm_rows = lu_df[lu_df.CODE_TYPE == 'MATERIAL_INVOLVEMENT_TYPE']
    fm_lu = fm_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    fm_lu.columns = ['FUEL_MODEL_IDENTIFIER','FUEL_MODEL']
    df = df.merge(fm_lu, how='left')
    print(df.shape)
    
    # AREA MEASUREMENTS & Conversion to Acres:
    area_uom_rows = lu_df[lu_df.CODE_TYPE == 'AREA_UOM']
    area_uom_lu = area_uom_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    # AREA_MEASUREMENT
    area_uom_lu.columns = ['CURR_INC_AREA_UOM_IDENTIFIER','CURR_INC_AREA_UOM']
    df = df.merge(area_uom_lu, how='left')
    print(df.shape)

    #PROJ_AREA_MEASUREMENT
    area_uom_lu.columns = ['PROJ_INC_AREA_UOM_IDENTIFIER','PROJ_INC_AREA_UOM']
    df = df.merge(area_uom_lu, how='left')
    print(df.shape)
    
    # convert to 'ACRES'
    df.loc[df['CURR_INC_AREA_UOM'] == 'Acres','ACRES'] = df['CURR_INCIDENT_AREA']
    df.loc[df['CURR_INC_AREA_UOM'] == 'Square Miles','ACRES'] = df['CURR_INCIDENT_AREA'] * 640
    
    # team type
    tt_rows = lu_df[lu_df.CODE_TYPE == 'TT']
    tt_lu = tt_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    tt_lu.columns = ['INC_MGMT_ORG_IDENTIFIER','IMT_MGMT_ORG_DESC'] # column name matches historical field
    df = df.merge(tt_lu, how='left')
    print(df.shape)
    
    # time zone
    tz_rows = lu_df[lu_df.CODE_TYPE == 'WORLD_TIME_ZONE']
    tz_lu = tz_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    tz_lu.columns = ['LOCAL_TIMEZONE_IDENTIFIER','LOCAL_TIMEZONE']
    df = df.merge(tz_lu, how='left')
    print(df.shape)
    
    # principal meridian
    pm_rows = lu_df[lu_df.CODE_TYPE == 'PRINCIPAL_MERIDIAN']
    pm_lu = pm_rows[['LUCODES_IDENTIFIER','CODE_NAME']]
    pm_lu.columns = ['POO_LD_PM_IDENTIFIER','POO_LD_PM']
    pm_lu.shape
    df = df.merge(pm_lu, how='left')
    print(df.shape)
    
    # state code/state name
    st_df = pd.read_csv('../../data/out/COMMONDATA_STATES_2014.csv')
    st_df['STATE_CODE'] = st_df.STATE_CODE.apply(pd.to_numeric, args=('coerce',))
    st_lu = st_df[['STATE_CODE','STATE','STATE_NAME']]
    st_lu = st_lu.dropna(axis=0,how='any')
    st_lu.columns = ['POO_STATE_CODE','POO_STATE','POO_STATE_NAME']
    df = df.merge(st_lu, how='left')
    print(df.shape)
    
    sc = {'C': True, 'S': False}
    df['COMPLEX'] = False
    df.COMPLEX = df.SINGLE_COMPLEX_FLAG.map(sc, na_action=None)
    
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
    
def _create_incident_id(df):
    dfinc = df.sort_values(['INC_IDENTIFIER','REPORT_TO_DATE']).groupby('INC_IDENTIFIER').nth(-1).reset_index()
    dfinc['INCIDENT_ID'] = dfinc.START_YEAR.astype(str) + '_' + dfinc.INCIDENT_NUMBER.astype(str).str.strip() + '_' + \
                        dfinc.INCIDENT_NAME.astype(str).str.strip().str.upper()
    g1 = dfinc.groupby(['INCIDENT_ID']).size().reset_index(name="num_rows")
    #print("Duplicate INC Incident Identifiers:") # no longer splitting on incident_number
    #print(g1.loc[g1.num_rows>1])
    dfIDxref = dfinc[['INC_IDENTIFIER','INCIDENT_ID']]
    print(df.shape)
    df = pd.merge(df, dfIDxref, on=['INC_IDENTIFIER'], how='left')
    print(df.shape)
    return df

def _latitude_longitude_updates(df):
    curr_loc = pd.read_csv('../../data/raw/latlong_clean/2014_cleaned_ll-fod.csv')
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
    dfc_str = pd.read_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_AFFECTED_STRUCTS_{}.csv'.format(curr_timespan))
    dfc_str = dfc_str.loc[:, ~dfc_str.columns.str.contains('^Unnamed')]
    
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
    dfc_res = pd.read_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_RES_UTILIZATIONS_{}.csv'.format(curr_timespan))
    dfc_res = dfc_res.loc[:, ~dfc_res.columns.str.contains('^Unnamed')]
    
    res_rows = lu_tbl[lu_tbl.CODE_TYPE == 'RESOURCE_TYPE']
    res_lu = res_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    res_lu.columns = ['RESTYP_IDENTIFIER','RESTYP']

    dfc_res = dfc_res.merge(res_lu, how='left')
    dfc_res.to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_RES_UTILIZATIONS_{}.csv'.format(curr_timespan))
    
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
    dfc_cslty = pd.read_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_CSLTY_ILLNESSES_{}.csv'.format(curr_timespan))
    dfc_cslty = dfc_cslty.loc[:, ~dfc_cslty.columns.str.contains('^Unnamed')]

    # Get casualty/illness values
    cit_rows = lu_tbl[lu_tbl.CODE_TYPE == 'CASUALTY_ILLNESS_TYPE']
    cit_lu = cit_rows[['LUCODES_IDENTIFIER','ABBREVIATION']]
    cit_lu.columns = ['CIT_IDENTIFIER','CIT']

    # Merge in casualty/illness type
    dfc_cslty = dfc_cslty.merge(cit_lu, on='CIT_IDENTIFIER',how='left')
    dfc_cslty = dfc_cslty.loc[dfc_cslty.CIT.isin(['II','E','F'])] # restrict to Injury/Illness, Evacuation, Fatality
    
    # Sum public/responder to simplify pivot
    df_qtyrep = dfc_cslty.groupby(['INC209R_IDENTIFIER','CIT']).QTY_THIS_REP_PERIOD.sum().reset_index(name="THIS_PERIOD_SUM")
    df_todate = dfc_cslty.groupby(['INC209R_IDENTIFIER','CIT']).QTY_TO_DATE.sum().reset_index(name="TO_DATE_SUM")
    df_cslty = df_qtyrep.merge(df_todate,on=['INC209R_IDENTIFIER','CIT'])
    
    # pivot table and rename columns
    df_cslty_piv = df_cslty.pivot_table(index=['INC209R_IDENTIFIER'],columns=['CIT'],values=['THIS_PERIOD_SUM','TO_DATE_SUM'])
    df_cslty_piv.columns = ["_".join((i,j)) for i,j in df_cslty_piv.columns]
    df_cslty_piv.reset_index(inplace=True)
    df_cslty_piv.fillna(0, inplace=True)
    df_cslty_piv.head()        

    df_cslty_piv.columns = ['INC209R_IDENTIFIER','NUM_EVACUATED','FATALITIES_THIS_PERIOD','INJURIES','NUM_EVAC_TO_DATE',\
                        'FATALITIES','INJURIES_TO_DATE']
    df_cslty_piv.loc[df_cslty_piv.NUM_EVACUATED > 0, 'EVACUATION_IN_PROGRESS'] = True
    return df_cslty_piv

def current_merge_prep():
    df = pd.read_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_REPORTS_{}.csv'.format(curr_timespan), parse_dates=True,\
                     low_memory=True)
    lu_df = pd.read_csv('../../data/out/SIT209_LOOKUP_CODES.csv')
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    df = _clean_and_format_date_and_time_fields(df)
    df = _derive_new_fields(df)
    
    df = _general_field_cleaning(df)
    df = _standardized_field_cleaning(df,lu_df)
    df = _create_incident_id(df)
    df = _latitude_longitude_updates(df)

    # save id xref and output files
    dfIDxref = df[['INC_IDENTIFIER','INCIDENT_ID']]
    dfIDxref = dfIDxref.drop_duplicates()
    df_str_ext = _get_str_ext(lu_df)
    df_ext = pd.merge(df,df_str_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After structure merge: {}".format(df_ext.shape))
    df_res_ext = _get_res_ext(lu_df)
    df_ext = pd.merge(df_ext,df_res_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After resource merge: {}".format(df_ext.shape))
    df_cslty_ext = _get_curr_cslty_ext(lu_df)
    df_ext = pd.merge(df_ext,df_cslty_ext,on=['INC209R_IDENTIFIER'],how='left')
    print("After casualty merge: {}".format(df_ext.shape))
    print("Current System merge preparation complete. {}".format(df_ext.shape))
    
    df_ext.to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_REPORTS_{}_cleaned.csv'.format(curr_timespan))
