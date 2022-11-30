import os
import ics209util
import pandas as pd
import numpy as np
import earthpy as et

hist_timespan = '2001to2013'
data_dir = os.path.join(et.io.HOME, 'data')
out_dir = os.path.join(data_dir, 'out')

def _split_duplicate_incident_numbers(df):
    df['SEQ_NUM'] = "1"
    
    # fix incident number errors
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Lay Spring")), 'INCIDENT_NAME'] = "Lazy Spring"
    df.loc[((df['INCIDENT_NUMBER'] == "CA-BTU-") & (df['INCIDENT_NAME'] == "Panther")), 'INCIDENT_NUMBER'] = "CA-BTU-005648"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011141") & (df['INCIDENT_NAME'] == "Pecan Creek")), 'INCIDENT_NUMBER'] = \
            "TX-TXS-011140"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011221") & (df['INCIDENT_NAME'] == "Tankawa Bluff")), 'INCIDENT_NUMBER'] = \
            "TX-TXS-011222"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011263") & (df['INCIDENT_NAME'] == "Four Six")), 'INCIDENT_NUMBER'] = \
            "TX-TXS-011262"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011379") & (df['INCIDENT_NAME'] == "Crab Prairie")), 'INCIDENT_NUMBER'] = \
            "TX-TXS-011378"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011314") & (df['INCIDENT_NAME'] == "EPW")), 'INCIDENT_NUMBER'] = "TX-TXS-011313"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011393") & (df['INCIDENT_NAME'] == "S389")), 'INCIDENT_NUMBER'] = "TX-TXS-011392"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011397") & (df['INCIDENT_NAME'] == "Luther Hill")), 'INCIDENT_NUMBER'] = \
            "TX-TXS-011396"
    df.loc[(df['INCIDENT_NUMBER'] == 'SNYDER CANYON'),'INCIDENT_NUMBER'] = '14620-9261-1071'
    df.loc[(df['INCIDENT_NUMBER'] == '14620-9261-1071'),'INCIDENT_NAME'] = 'SNYDER CANYON'
    df.loc[(df['INCIDENT_NUMBER'] == 'REDINGTON'),'INCIDENT_NUMBER'] = 'AZ-AZS-03-0181'
    df.loc[(df['INCIDENT_NUMBER'] == 'AZ-AZS-03-0181'),'INCIDENT_NAME'] = 'REDINGTON'
    df.loc[(df['INCIDENT_NUMBER'] == 'MULHALL'),'INCIDENT_NUMBER'] = 'OK-OKS-CW#201'
    df.loc[(df['INCIDENT_NUMBER'] == 'OK-OKS-CW#201'),'INCIDENT_NAME'] = 'MULHALL'
    df.loc[(df['INCIDENT_NAME'] == 'BLOSSOM COMPLEX'),'INCIDENT_NUMBER'] = 'P6-B1KB-011' #combined both listed P6-B1KB- + 011
    df.loc[(df['INCIDENT_NUMBER'] == '0R-WSA-0007'),'INCIDENT_NUMBER'] = 'OR-WSA-0007' #change 0 to 'O'
    df.loc[(df['INCIDENT_NUMBER'] == '0R-ORS-98S-023'),'INCIDENT_NUMBER'] = 'OR-ORS-98S-023' #change 0 to 'O'
    df.loc[(df['INCIDENT_NUMBER'] == '0K-0SA-005084'),'INCIDENT_NUMBER'] = 'OK-0SA-005084' #change 0 to 'O'
    df.loc[(df['INCIDENT_NUMBER'] == '000128'),'INCIDENT_NUMBER'] = 'AK-FAS-011128' #one sitrep incomplete
    df.loc[(df['INCIDENT_NUMBER'] == '110006'),'INCIDENT_NUMBER'] = 'MO-MDC-110006' # Wick Hollow one sitrep incomplete
    df.loc[(df['INCIDENT_NUMBER'] == '1056'),'INCIDENT_NUMBER'] = 'OR-DEF-1056' # WIZARD one sitrep incomplete
    
    # data swap next 2 rows
    df.loc[((df['INCIDENT_NUMBER'] == "TROUT") & (df['INCIDENT_NAME'] == "ID-NPF-077")), 'INCIDENT_NUMBER'] = "ID-NPF-077"
    df.loc[((df['INCIDENT_NUMBER'] == "ID-NPF-077") & (df['INCIDENT_NAME'] == "ID-NPF-077")), 'INCIDENT_NUMBER'] = "TROUT"
    
    # split duplicates with seq#
    df.loc[((df['INCIDENT_NUMBER'] == "GA-GAS-011011") & (df['INCIDENT_NAME'] == "April Tornado Disaster")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D2") & (df['INCIDENT_NAME'] == "Dierks")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D2") & (df['INCIDENT_NAME'] == "Red Barn")), 'SEQ_NUM'] = "3"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "CR 46")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Begley  Creek")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Lazy Spring")), 'SEQ_NUM'] = "3"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Mill Creek")), 'SEQ_NUM'] = "4"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Valley Springs")), 'SEQ_NUM'] = "5"
    df.loc[((df['INCIDENT_NUMBER'] == "AR-ARS-D7") & (df['INCIDENT_NAME'] == "Marys Hollow")), 'SEQ_NUM'] = "6"
    df.loc[((df['INCIDENT_NUMBER'] == "FL-FNF-011002") & (df['INCIDENT_NAME'] == "Shanty 32")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "FL-FNF-011001") & (df['INCIDENT_NAME'] == "Fat Boy")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "ID-2PN-012019") & (df['INCIDENT_NAME'] == "Power County Assist 1")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "ID-CWF-700") & (df['INCIDENT_NAME'] == "Fire Creek")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "ID-IPF-004019") & (df['INCIDENT_NAME'] == "Little Devil")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "OK-OKS-") & (df['INCIDENT_NAME'] == "Lone Pine")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "OK-OKS-") & (df['INCIDENT_NAME'] == "Lone Pine")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "OK-OKS-40021") & (df['INCIDENT_NAME'] == "Elmore City")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "OR-UPF-008083") & (df['INCIDENT_NAME'] == "Rattle Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-000016") & (df['INCIDENT_NAME'] == "EDWARDS COMPLEX")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-00013") & (df['INCIDENT_NAME'] == "Calvert Creek")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-00014") & (df['INCIDENT_NAME'] == "JOHNSON RANCH")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011048") & (df['INCIDENT_NAME'] == "Twin Peaks Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011084") & (df['INCIDENT_NAME'] == "Hohertz")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011242") & (df['INCIDENT_NAME'] == "NW Branch - W. Texas IA")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011291") & (df['INCIDENT_NAME'] == "314")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-011435") & (df['INCIDENT_NAME'] == "Pat Gross")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-012020") & (df['INCIDENT_NAME'] == "The Kenton Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-04018") & (df['INCIDENT_NAME'] == "Eagle Spring")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-066034") & (df['INCIDENT_NAME'] == "McFarland Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['INCIDENT_NUMBER'] == "TX-TXS-88031") & (df['INCIDENT_NAME'] == "Callisburg")), 'SEQ_NUM'] = "2"

    df = df.drop(df[(df.INCIDENT_NUMBER == "AK-FAS-0098")].index) #sues test - 1 sitrep
    return df

def _clean_and_format_date_and_time_fields(df):
    # report_to_date (combine report date + hour)
    df['REPORT_TO_DATE'] = df.REPORT_DATE + ' ' + df.HOUR.astype(str).str.zfill(4)
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE)
    
    # fix erroneous dates
    df.loc[df['INCIDENT_NUMBER'] == 'GA-CHF-02054','DEMOBE_START'] = '2002-03-28 00:00:00'
    df.loc[df['INCIDENT_NUMBER'] == 'CA-LPF-1757', 'EXP_CONTAIN'] = np.nan #value before START_DATE/Year=2400
    
    df['CY'] = df['REPORT_DATE'].astype('datetime64[ns]').dt.year
    
    df['START_DATE'] = pd.to_datetime(df['START_DATE'], errors='coerce')
    df['START_YEAR'] = df['START_DATE'].dt.year
    df.loc[df.START_YEAR.isnull(), 'START_YEAR'] = df.CY
    
    # reformat dates
    df['APPROVED_DATE'] = pd.to_datetime(df.APPROVED_DATE,infer_datetime_format=True)
    df['CONTROLLED_DATE'] = pd.to_datetime(df.CONTROLLED_DATE,infer_datetime_format=True)
    df['DEMOBE_START'] = pd.to_datetime(df.DEMOBE_START,infer_datetime_format=True)
    df['EXP_CONTAIN'] = pd.to_datetime(df.EXP_CONTAIN,infer_datetime_format=True)
    df['LAST_EDIT'] = pd.to_datetime(df.LAST_EDIT,infer_datetime_format=True)
    df['SENT_DATE'] = pd.to_datetime(df.SENT_DATE,infer_datetime_format=True)
    df['START_DATE'] = pd.to_datetime(df.START_DATE,infer_datetime_format=True)
    return df
    
    
def _derive_new_fields(df):
    # Fire Event ID
    df.loc[df.TYPE_INC.isin(['WF','WFU','RX']),'FIRE_EVENT_ID'] = df.INCIDENT_NUMBER.astype(str).str.strip() + "|" + \
               df.START_YEAR.astype(int).astype(str) + "|" + df.SEQ_NUM
    
    # ACRES
    am = {'ACRES': 'Acres','SQ MILES':'Square Miles','HECTARES':'Hectares'}
    df['AREA_MEASUREMENT'] = df['AREA_MEASUREMENT'].map(am)
    df.loc[df['AREA_MEASUREMENT'] == 'Acres','ACRES'] = df['AREA']
    df.loc[df['AREA_MEASUREMENT'] == 'Hectares','ACRES'] = df['AREA'] * 2.47105
    df.loc[df['AREA_MEASUREMENT'] == 'Square Miles','ACRES'] = df['AREA'] * 640
    
    # Fire Behavior Flags
    df['FB_BACKING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('backing')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('backing')))
    df['FB_CREEPING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('creeping')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('creeping')))
    df['FB_CROWNING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('crown')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('crown')))
    df['FB_FLANKING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('flanking')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('flanking')))
    df['FB_RUNNING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('run')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('run')))
    df['FB_SMOLDERING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('smolder')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('smolder')))
    df['FB_SPOTTING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('spot')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('spot')))
    df['FB_TORCHING'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('torch')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('torch')))
    df['FB_WIND_DRIVEN'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('wind')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('wind')))
    df['FB_ACTIVE'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('active')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('active')))
    df['FB_MINIMAL'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('minimal')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('minimal')))
    df['FB_MODERATE'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('moderate')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('moderate')))
    df['FB_EXTREME'] = ((df.GACC_OBS_FIRE_BEHAVE.str.lower().str.contains('extreme')) | 
                         (df.OBS_FIRE_BEHAVE.str.lower().str.contains('extreme')))
    
    # WEATHER_CONCERNS_NARR
    df['WEATHER_CONCERNS_NARR'] = ""
    df.loc[(df.C_RH.notnull()|df.C_TEMP.notnull()|df.C_WIND_SPEED.notnull()|df.C_WIND_DIRECTION.notnull()),\
                      'WEATHER_CONCERNS_NARR'] = "Current Weather RH: " + df.C_RH.astype(str).str.strip() +\
                                                 " TEMP: " + df.C_TEMP.astype(str).str.strip() + \
                                                 " WS: " + df.C_WIND_SPEED.astype(str).str.strip() + \
                                                 " WD: " + df.C_WIND_DIRECTION.astype(str).str.strip()
    df.loc[(df.F_RH.notnull()|df.F_TEMP.notnull()|df.F_WIND_SPEED.notnull()|df.F_WIND_DIRECTION.notnull()),\
                      'WEATHER_CONCERNS_NARR'] = df.WEATHER_CONCERNS_NARR + \
                                                 " Forecasted Conditions RH: " + df.C_RH.astype(str).str.strip() + \
                                                 " TEMP: " + df.C_TEMP.astype(str).str.strip() + \
                                                 " WS: " + df.C_WIND_SPEED.astype(str).str.strip() + \
                                                 " WD: " + df.C_WIND_DIRECTION.astype(str).str.strip()
    
    df['CURRENT_THREAT_NARR'] = ics209util.combine_text_fields(df,'COMMUNITIES_THREATENED_12','COMMUNITIES_THREATENED_24',
                                            'COMMUNITIES_THREATENED_48','COMMUNITIES_THREATENED_72','RES_THREAT')
    df['CRIT_RES_NEEDS_NARR'] = ics209util.combine_text_fields(df,'CRITICAL_RES','CRITICAL_RES24','CRITICAL_RES48',
                                                               'CRITICAL_RES72')
    df['PROJECTED_ACTIVITY_NARR'] = ics209util.combine_text_fields(df,'PROJECTED_MOVEMENT','PROJECTED_MOVEMENT24',
                                                            'PROJECTED_MOVEMENT48','PROJECTED_MOVEMENT72')
    return df
    
def _general_field_cleaning(df):
    
    # estimated final area (remove non-numeric characters)
    df['EST_FINAL_AREA'] = df['EST_FINAL_AREA'].str.replace('\D','')
    
    # new field containing just numeric values. If value represented with '-' dash, take min as estimate.
    df['LINE_TO_BUILD_NUM'] = df['LINE_TO_BUILD'].str.extract('([\d\,\.]+)',expand=False)
    df['LINE_TO_BUILD_NUM'] = df['LINE_TO_BUILD_NUM'].str.replace(',','')
    df['LINE_TO_BUILD_NUM'] = df['LINE_TO_BUILD_NUM'].astype('float64')
    
    # upper case incident name
    df['INCIDENT_NAME'] = df['INCIDENT_NAME'].str.upper()
    
    # text field cleaning
    df['CRITICAL_RES'] = df['CRITICAL_RES'].str.replace('\s?\-\/','|')
    df['COOP_AGENCIES'] = df.COOP_AGENCIES.apply(np.vectorize(ics209util.clean_narrative_text))
    df['GACC_OBS_FIRE_BEHAVE'] = df.GACC_OBS_FIRE_BEHAVE.apply(np.vectorize(ics209util.clean_narrative_text))
    df['GACC_PLANNED_ACTIONS'] = df.GACC_PLANNED_ACTIONS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['GACC_REMARKS'] = df.GACC_REMARKS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['GACC_SIG_EVENT'] = df.GACC_SIG_EVENT.apply(np.vectorize(ics209util.clean_narrative_text))
    df['MAJOR_PROBLEMS'] = df.MAJOR_PROBLEMS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['OBS_FIRE_BEHAVE'] = df.OBS_FIRE_BEHAVE.apply(np.vectorize(ics209util.clean_narrative_text))
    df['PLANNED_ACTIONS'] = df.PLANNED_ACTIONS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['REMARKS'] = df.REMARKS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['RES_BENEFITS'] = df.RES_BENEFITS.apply(np.vectorize(ics209util.clean_narrative_text))
    df['RES_THREAT'] = df.RES_THREAT.apply(np.vectorize(ics209util.clean_narrative_text))
    df['SIG_EVENT'] = df.SIG_EVENT.apply(np.vectorize(ics209util.clean_narrative_text))
    df['TARGETS_MET'] = df.TARGETS_MET.apply(np.vectorize(ics209util.clean_narrative_text))
    return df
    

def _standardized_fields(df):
    # convert y/n to booleans
    yn = {'Y': True, 'N': False}
    df.ACTIVE = df.ACTIVE.map(yn)
    df.COMPLEX = df.COMPLEX.map(yn)
    df.EVACUATION_IN_PROGRESS = df.EVACUATION_IN_PROGRESS.map(yn)
    df.NO_EVACUATION = df.NO_EVACUATION.map(yn)
    df.NO_LIKELY = df.NO_LIKELY.map(yn)
    df.POTENTIAL = df.POTENTIAL.map(yn)
    
    # cause
    cm = {'H':'H','L':'L','U':'U','N':'O'}
    df['CAUSE'] = df['CAUSE'].map(cm)
    
    # IMT Type Description
    df['IMT_TYPE'] = df['IMT_TYPE'].str.upper().str.extract('([A-Z0-9])',expand=False)
    tt = {'1':'Type 1 Team','2':'Type 2 Team','3':'Type 3 Team','4':'Type 3 IC','5':'Type 4 IC','6':'Type 5 IC',
      '7':'Type 1 IC','8':'Type 2 IC','A':'FUM1','B':'FUM2','C':'Area Command','D':'Unified Command','E':'SOPL',
      'F':'FUMT'}
    df['IMT_MGMT_ORG_DESC'] = df['IMT_TYPE'].map(tt)
    
    # percent/mma convert to current values
    pm = {'P': 'PCT', 'M': 'MMA'}
    df['PCT_CONT_COMPL_UOM_ABBREV'] = df['PERCENT_MMA'].map(pm)
    df['PCT_CONT_COMPL_UOM_ABBREV'].value_counts()
    
    # suppression method abbreviation and full description
    ss_abbrev = {'MM':'M','CC':'C','PZ': 'PZP','FS':'FS'}
    df['SUPPRESSION_METHOD'] = df['SUPPRESSION_METHOD'].map(ss_abbrev)
    ss = {'C': 'Confine', 'FS': 'Full Suppression','PZP': 'Point Zone Protection', 'M': 'Monitor'}
    df['SUPPRESSION_METHOD_FULLNAME'] = df['SUPPRESSION_METHOD'].map(ss)
    
    # fix erroneous incident type
    df.loc[df.INCIDENT_NUMBER=='OH-WAF-01', 'TYPE_INC'] = 'OT'
    itidmap = {'EQ':9855,'FL':9856,'HM':9858,'HU':9860,'TO':9867,'WF':9851,'WFU':1,'RX':2,'SAR':9864,'OT':9925,\
          'LE':9863,'STR':9925,'MC':9925,'USR':9864,'BAR':9925,'OS':9925}
    df['INCTYP_IDENTIFIER'] = df['TYPE_INC'].map(itidmap)
    
    fmmap = {1.0:'Short Grass (1 foot)', 2.0:'Timber (Grass and Understory)', 3.0:'Tall Grass (2.5 feet)',\
        4.0:'Chaparral (6 feet)',5.0:'Brush (2 feet)',6.0:'Dormant Brush, Hardwood Slash',7.0:'Southern Rough',\
        8.0:'Closed Timber Litter',9.0:'Hardwood Litter',10.0:'Timber (Litter and Understory)',\
        11.0:'Light Logging Slash',12.0:'Medium Logging Slash',13.0:'Heavy Logging Slash'}
    df['FUEL_MODEL'] = df['PRIMARY_FUEL_MODEL'].map(fmmap)
    
    st_df = pd.read_csv(os.path.join(out_dir,
                                     'COMMONDATA_STATES_2014.csv'))
    st_lu = st_df[['STATE','STATE_NAME']]
    st_lu = st_lu.dropna(axis=0,how='any')
    st_lu.columns = ['UN_USTATE','POO_STATE_NAME']

    df = df.merge(st_lu, how='left')
    return df
    
def _ks_merge_purge_duplicates(df):
    df['REPORT_DATE'] = pd.to_datetime(df.REPORT_DATE)
    df['HOUR'] = df['HOUR'].fillna(0.0)
    df['HOUR'] = df['HOUR'].astype(int)
    df = df.drop_duplicates()
    df.to_csv(os.path.join(out_dir,
                           "IMSR_IMSR_209_INCIDENTS_{}_cleaned.csv".format(hist_timespan)))
    
    df_short = pd.read_excel(os.path.join(data_dir,
                                          'raw',
                                          'excel',
                                          'Short1999to2013v2.xlsx'))
    df_short['INCIDENT_NAME'] = df_short['INCIDENT_NAME'].astype(str)
    df_short['INCIDENT_NUMBER'] = df_short['INCIDENT_NUMBER'].astype(str)
    df_short = df_short.drop_duplicates()
    
    df_short['HOUR'] = df_short['HOUR'].fillna(0.0)
    df_short['HOUR'] = df_short['HOUR'].astype(int)
    df_short['REPORT_DATE'] = pd.to_datetime(df_short.REPORT_DATE)
    short_cols = df_short[['REPORT_DATE','HOUR','INCIDENT_NUMBER','INCIDENT_ID_KS','KS_COMPLEX_NAME',
                           'INCIDENT_NAME_CORRECTED','INCIDENT_NUMBER_CORRECTED','START_DATE_CORRECTED']].copy()
    short_cols.columns = ['REPORT_DATE','HOUR','INCIDENT_NUMBER','INCIDENT_ID','COMPLEX_NAME',
                           'INCIDENT_NAME_CORRECTED','INCIDENT_NUMBER_CORRECTED','START_DATE_CORRECTED']
    short_cols['INCIDENT_ID'] = short_cols.INCIDENT_ID.astype(str).str.strip().str.upper()
    
    # merge
    df = pd.merge(df, short_cols, on=['INCIDENT_NUMBER','REPORT_DATE','HOUR'], how='left')
    
    # Save file with deleted records:
    df_notinKS = df.loc[df.INCIDENT_ID.isnull() & df.TYPE_INC.isin(['WF','WFU'])]
    df_notinKS.to_csv(os.path.join(out_dir,
                                   "ics209_sitreps_deleted_hist2_{}.csv".format(hist_timespan)))
    # Save file merged with KS records as cleanedKS version
    df = df.loc[~df.INCIDENT_ID.isnull() | ~df.TYPE_INC.isin(['WF','WFU'])]
    df.loc[~df.TYPE_INC.isin(['WF','WFU']),'INCIDENT_ID'] = df.START_YEAR.astype(int).astype(str) + '_' \
                                    + df.INCIDENT_NUMBER.astype(str).str.strip() + '_' + \
                                    df.INCIDENT_NAME.astype(str).str.strip()
    
    # set complex flag (see validation logic in jupyter notebook)
    df.loc[(df.COMPLEX_NAME.notnull() & df.TYPE_INC.isin(['WF','WFU'])),'COMPLEX'] = True
    df.loc[(df.COMPLEX.isnull() & df.TYPE_INC.isin(['WF','WFU'])),'COMPLEX'] = False
    return df

def _latitude_longitude_updates(df):
    hist_loc = pd.read_csv(os.path.join(data_dir,
                                        'raw',
                                        'latlong_clean',
                                        'historical_cleaned_ll-fod.csv'))
    df = df.merge(hist_loc, on=['FIRE_EVENT_ID'],how='left')
    # Set the Update Flag
    df.loc[df.lat_c.notnull(),'LL_UPDATE'] = True
    # Case #1: Update lat/long using estimate
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'LATITUDE'] = df.lat_c # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'LONGITUDE'] = df.long_c # set longitude to nan
    # Case #2: FOD Update available
    df.loc[df.FOD_LATITUDE.notnull(),'LATITUDE'] = df.FOD_LATITUDE
    df.loc[df.FOD_LONGITUDE.notnull(),'LONGITUDE'] = df.FOD_LONGITUDE
    # Case #3: Unable to fix so set to null
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'LATITUDE'] = np.nan # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'LONGITUDE'] = np.nan # set longitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'LL_CONFIDENCE'] = 'N'
    
    # All-hazards lat/long fixes (primarily Hurricane)
    df.loc[df.INCIDENT_ID=='2002_CA-OSC-226_SUPER TYPHOON PONGSONA','LATITUDE'] = 7.4497222
    df.loc[df.INCIDENT_ID=='2002_CA-OSC-226_SUPER TYPHOON PONGSONA','LONGITUDE'] = 151.864444
    df.loc[df.INCIDENT_ID=='2002_CA-OSC-226_SUPER TYPHOON PONGSONA','LL_CONFIDENCE'] = 'M'
    df.loc[df.INCIDENT_ID=='2002_CA-OSC-226_SUPER TYPHOON PONGSONA','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA','LATITUDE'] = 35.9082
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA','LONGITUDE'] = -75.6757
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA\/CALO','LATITUDE'] = 35.9082
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA\/CALO','LONGITUDE'] = -75.6757
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA\/CALO','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2003_VA-EIC-000002_ISABEL RECOVERY -CAHA\/CALO','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2004_FL-BIP-04001_HURRICANE CHARLEY','LATITUDE'] = 25.4687
    df.loc[df.INCIDENT_ID=='2004_FL-BIP-04001_HURRICANE CHARLEY','LONGITUDE'] = -80.4776
    df.loc[df.INCIDENT_ID=='2004_FL-BIP-04001_HURRICANE CHARLEY','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2004_FL-BIP-04001_HURRICANE CHARLEY','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2005_CA-RRU-105076_SCHOOL','LATITUDE'] = 33.5025 #hazmat
    df.loc[df.INCIDENT_ID=='2005_CA-RRU-105076_SCHOOL','LONGITUDE'] = -117.1168
    df.loc[df.INCIDENT_ID=='2005_CA-RRU-105076_SCHOOL','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2005_CA-RRU-105076_SCHOOL','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2005_FL-FEM-000001_DENNIS PREPAREDNESS HOMESTEAD','LATITUDE'] = 25.4687
    df.loc[df.INCIDENT_ID=='2005_FL-FEM-000001_DENNIS PREPAREDNESS HOMESTEAD','LONGITUDE'] = -80.4776
    df.loc[df.INCIDENT_ID=='2005_FL-FEM-000001_DENNIS PREPAREDNESS HOMESTEAD','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2005_FL-FEM-000001_DENNIS PREPAREDNESS HOMESTEAD','LL_UPDATE'] = True
    df.loc[df.INCIDENT_ID=='2007_WA-OLF-000299_OLF STORM07','LATITUDE'] = 47.7501
    df.loc[df.INCIDENT_ID=='2007_WA-OLF-000299_OLF STORM07','LONGITUDE'] = -123.751
    df.loc[df.INCIDENT_ID=='2007_WA-OLF-000299_OLF STORM07','LL_CONFIDENCE'] = 'H'
    df.loc[df.INCIDENT_ID=='2007_WA-OLF-000299_OLF STORM07','LL_UPDATE'] = True
   
    return df

def _get_str_ext(dfh_xref):
    # read in structures table
    dfh_str = pd.read_csv(os.path.join(out_dir,
                                       "IMSR_IMSR_209_INCIDENT_STRUCTURES_{}.csv".format(hist_timespan)))
    dfh_str = dfh_str.loc[:, ~dfh_str.columns.str.contains('^Unnamed')]
    
    #Fix misc incident number issues
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == 'SNYDER CANYON'),'IM_INCIDENT_NUMBER'] = '14620-9261-1071' # swap
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == 'REDINGTON'),'IM_INCIDENT_NUMBER'] = 'AZ-AZS-03-0181' # swap
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == 'MULHALL'),'IM_INCIDENT_NUMBER'] = 'OK-OKS-CW#201' # swap 
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == 'P6-B1KB-'),'IM_INCIDENT_NUMBER'] = 'P6-B1KB-011' # incomplete inc #
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '011'),'IM_INCIDENT_NUMBER'] = 'P6-B1KB-011' # incomplete inc #
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '0R-WSA-0007'),'IM_INCIDENT_NUMBER'] = 'OR-WSA-0007' #change 0 to 'O'
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '0R-ORS-98S-023'),'IM_INCIDENT_NUMBER'] = 'OR-ORS-98S-023' #change 0 to 'O'
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '0K-0SA-005084'),'IM_INCIDENT_NUMBER'] = 'OK-0SA-005084' #change 0 to 'O'
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '128'),'IM_INCIDENT_NUMBER'] = 'AK-FAS-011128' # incomplete inc #
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '110006'),'IM_INCIDENT_NUMBER'] = 'MO-MDC-110006' # incomplete inc #
    dfh_str.loc[(dfh_str['IM_INCIDENT_NUMBER'] == '1056'),'IM_INCIDENT_NUMBER'] = 'OR-DEF-1056' # incomplete inc #
    
    # Convert Report Date to Timestamp and drop Hour
    dfh_str['REPORT_TO_DATE'] = dfh_str.IM_REPORT_DATE.astype(str) + ' ' + dfh_str.IM_HOUR.astype(str).str.zfill(4)
    dfh_str['REPORT_TO_DATE'] = pd.to_datetime(dfh_str.REPORT_TO_DATE, errors='coerce')
    dfh_str = dfh_str.drop('IM_HOUR',axis=1)
    dfh_str.columns = dfh_str.columns.str.replace('IM_INCIDENT_NUMBER','INCIDENT_NUMBER')
    dfh_xref['REPORT_TO_DATE'] = pd.to_datetime(dfh_xref.REPORT_TO_DATE, errors='coerce')

    # perform the merge with UID
    dfh_str = dfh_str.merge(dfh_xref,on=['INCIDENT_NUMBER','REPORT_TO_DATE'],how='left')
    
    dfh_str = dfh_str.loc[dfh_str.INCIDENT_ID.notnull()]
    
    # pivot the table
    dfh_str_piv = dfh_str.pivot_table(index = ['INCIDENT_ID','REPORT_TO_DATE'], columns='STRUCTURE_TYPE',\
                                        values=['DAMAGED','DESTROYED','THREATENED'])
    dfh_str_piv.columns = ["_".join((i,j)) for i,j in dfh_str_piv.columns]

    dfh_str_piv = dfh_str_piv.fillna(0)
    dfh_str_piv.reset_index(inplace=True)
    
    # Forward fill missing values for damaged/destroyed columns
    rows = dfh_str_piv.shape[0]
    for i in range(0,rows-1):
        # Check to see if current rows belong to same incident
        if dfh_str_piv.iloc[i+1].INCIDENT_ID == dfh_str_piv.iloc[i].INCIDENT_ID:
            if dfh_str_piv.iloc[i+1].DAMAGED_COMM == 0 and dfh_str_piv.iloc[i].DAMAGED_COMM > 0:
                dfh_str_piv.loc[i+1,'DAMAGED_COMM'] = dfh_str_piv.loc[i,'DAMAGED_COMM'] #propagate prev value
            if dfh_str_piv.iloc[i+1].DAMAGED_OUTB == 0 and dfh_str_piv.iloc[i].DAMAGED_OUTB > 0:
                dfh_str_piv.loc[i+1,'DAMAGED_OUTB'] = dfh_str_piv.loc[i,'DAMAGED_OUTB'] #propagate prev value
            if dfh_str_piv.iloc[i+1].DAMAGED_PRIM == 0 and dfh_str_piv.iloc[i].DAMAGED_PRIM > 0:
                dfh_str_piv.loc[i+1,'DAMAGED_PRIM'] = dfh_str_piv.loc[i,'DAMAGED_PRIM'] #propagate prev value
            if dfh_str_piv.iloc[i+1].DESTROYED_COMM == 0 and dfh_str_piv.iloc[i].DESTROYED_COMM > 0:
                dfh_str_piv.loc[i+1,'DESTROYED_COMM'] = dfh_str_piv.loc[i,'DESTROYED_COMM'] #propagate prev value
            if dfh_str_piv.iloc[i+1].DESTROYED_OUTB == 0 and dfh_str_piv.iloc[i].DESTROYED_OUTB > 0:
                dfh_str_piv.loc[i+1,'DESTROYED_OUTB'] = dfh_str_piv.loc[i,'DESTROYED_OUTB'] #propagate prev value
            if dfh_str_piv.iloc[i+1].DESTROYED_PRIM == 0 and dfh_str_piv.iloc[i].DESTROYED_PRIM > 0:
                dfh_str_piv.loc[i+1,'DESTROYED_PRIM'] = dfh_str_piv.loc[i,'DESTROYED_PRIM'] #propagate prev value
     
    # Create total fields
    dfh_str_piv['STR_DAMAGED'] = dfh_str_piv.DAMAGED_COMM + dfh_str_piv.DAMAGED_OUTB + dfh_str_piv.DAMAGED_PRIM
    dfh_str_piv['STR_DESTROYED'] = dfh_str_piv.DESTROYED_COMM + dfh_str_piv.DESTROYED_OUTB + dfh_str_piv.DESTROYED_PRIM
    dfh_str_piv['STR_THREATENED'] = dfh_str_piv.THREATENED_COMM + dfh_str_piv.THREATENED_OUTB + dfh_str_piv.THREATENED_PRIM
    
    # Save output files
    dfh_str_piv.to_csv(os.path.join(out_dir,
                                    "IMSR_IMSR_209_INCIDENT_STRUCTURES_{}_UIDpivot_cln.csv".format(hist_timespan)))
    dfh_merge = dfh_str_piv[['INCIDENT_ID','REPORT_TO_DATE','STR_DAMAGED','STR_DESTROYED','STR_THREATENED',\
                            'DAMAGED_PRIM','DESTROYED_PRIM','THREATENED_PRIM','DAMAGED_COMM','DESTROYED_COMM','THREATENED_COMM']]
    # Rename the columns for merge
    dfh_merge.columns = dfh_merge.columns.str.replace('DAMAGED_PRIM','STR_DAMAGED_RES')
    dfh_merge.columns = dfh_merge.columns.str.replace('DESTROYED_PRIM','STR_DESTROYED_RES')
    dfh_merge.columns = dfh_merge.columns.str.replace('THREATENED_PRIM','STR_THREATENED_RES')
    dfh_merge.columns = dfh_merge.columns.str.replace('DAMAGED_COMM','STR_DAMAGED_COMM')
    dfh_merge.columns = dfh_merge.columns.str.replace('DESTROYED_COMM','STR_DESTROYED_COMM')
    dfh_merge.columns = dfh_merge.columns.str.replace('THREATENED_COMM','STR_THREATENED_COMM')
    return dfh_merge 
        
def _get_res_ext(uid_xref):
    dfh_res = pd.read_csv(os.path.join(out_dir,
                                       "IMSR_IMSR_209_INCIDENT_RESOURCES_{}.csv".format(hist_timespan)))
    dfh_res = dfh_res.loc[:, ~dfh_res.columns.str.contains('^Unnamed')]
    
    # compute total aerial
    dfh_res['TOTAL_AERIAL'] = dfh_res.BOMBARDIER.fillna(0) + dfh_res.C215.fillna(0) + dfh_res.C415.fillna(0) + \
            dfh_res.FIXED_WING.fillna(0) + dfh_res.HELICOPTER_TANKER.fillna(0) + dfh_res.LIGHT_AIR.fillna(0) + \
            dfh_res.SR_HELICOPTER_1.fillna(0) + dfh_res.SR_HELICOPTER_2.fillna(0) + dfh_res.SR_HELICOPTER_3.fillna(0) + \
            dfh_res.USAR_HELICOPTER.fillna(0)
            
    #Fix misc incident number issues
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == 'SNYDER CANYON'),'IM_INCIDENT_NUMBER'] = '14620-9261-1071'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == 'REDINGTON'),'IM_INCIDENT_NUMBER'] = 'AZ-AZS-03-0181'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == 'MULHALL'),'IM_INCIDENT_NUMBER'] = 'OK-OKS-CW#201'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == 'P6-B1KB-'),'IM_INCIDENT_NUMBER'] = 'P6-B1KB-011'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '011'),'IM_INCIDENT_NUMBER'] = 'P6-B1KB-011'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '0R-WSA-0007'),'IM_INCIDENT_NUMBER'] = 'OR-WSA-0007' #change 0 to 'O'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '0R-ORS-98S-023'),'IM_INCIDENT_NUMBER'] = 'OR-ORS-98S-023' #change 0 to 'O'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '0K-0SA-005084'),'IM_INCIDENT_NUMBER'] = 'OK-0SA-005084' #change 0 to 'O'
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '128'),'IM_INCIDENT_NUMBER'] = 'AK-FAS-011128' # incomplete inc #
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '110006'),'IM_INCIDENT_NUMBER'] = 'MO-MDC-110006' # incomplete inc #
    dfh_res.loc[(dfh_res['IM_INCIDENT_NUMBER'] == '1056'),'IM_INCIDENT_NUMBER'] = 'OR-DEF-1056' # incomplete inc #
    
    # select resource columns
    dfh_res = dfh_res[['AG_AID','IM_INCIDENT_NUMBER','IM_REPORT_DATE','IM_HOUR','TOTAL_PERSONNEL','TOTAL_AERIAL']]

    # Convert Report Date to Timestamp and drop Hour
    dfh_res['REPORT_TO_DATE'] = dfh_res.IM_REPORT_DATE.astype(str) + ' ' + dfh_res.IM_HOUR.astype(str).str.zfill(4)
    dfh_res['REPORT_TO_DATE'] = pd.to_datetime(dfh_res.REPORT_TO_DATE, errors='coerce')
    dfh_res = dfh_res.drop('IM_HOUR',axis=1)
    dfh_res.columns = dfh_res.columns.str.replace('IM_INCIDENT_NUMBER','INCIDENT_NUMBER')

    # Merge in Unique ID
    dfh_res = dfh_res.merge(uid_xref,on=['INCIDENT_NUMBER','REPORT_TO_DATE'],how='left')

    # Filter out rows where there is no ID & pivot
    dfh_res = dfh_res.loc[dfh_res.INCIDENT_ID.notnull()]
    dfh_res_piv = dfh_res.pivot_table(index = ['INCIDENT_ID','REPORT_TO_DATE'], columns='AG_AID',\
                                      values= ['TOTAL_PERSONNEL','TOTAL_AERIAL'],aggfunc=np.mean)
    dfh_res_piv.columns = ["_".join((i,j)) for i,j in dfh_res_piv.columns]

    # fill empty values and reset the index
    dfh_res_piv = dfh_res_piv.fillna(0)
    dfh_res_piv.reset_index(inplace=True)
    
    
    # Calculate the total personnel from all the agencies
    dfh_res_piv['TOTAL_PERSONNEL'] = dfh_res_piv.TOTAL_PERSONNEL_APHI + dfh_res_piv.TOTAL_PERSONNEL_BIA + \
                                    dfh_res_piv.TOTAL_PERSONNEL_BLM + dfh_res_piv.TOTAL_PERSONNEL_CDF + \
                                    dfh_res_piv.TOTAL_PERSONNEL_CNTY + dfh_res_piv.TOTAL_PERSONNEL_DDQ + \
                                    dfh_res_piv.TOTAL_PERSONNEL_DHS + dfh_res_piv.TOTAL_PERSONNEL_FWS + \
                                    dfh_res_piv.TOTAL_PERSONNEL_IA + dfh_res_piv.TOTAL_PERSONNEL_INTL + \
                                    dfh_res_piv.TOTAL_PERSONNEL_LGR + dfh_res_piv.TOTAL_PERSONNEL_NPS + \
                                    dfh_res_piv.TOTAL_PERSONNEL_OES + dfh_res_piv.TOTAL_PERSONNEL_ORC + \
                                    dfh_res_piv.TOTAL_PERSONNEL_OTHR + dfh_res_piv.TOTAL_PERSONNEL_PRI + \
                                    dfh_res_piv.TOTAL_PERSONNEL_ST + dfh_res_piv.TOTAL_PERSONNEL_USFS + \
                                    dfh_res_piv.TOTAL_PERSONNEL_WXW
    dfh_res_piv['TOTAL_AERIAL'] = dfh_res_piv.TOTAL_AERIAL_APHI + dfh_res_piv.TOTAL_AERIAL_BIA + \
                                    dfh_res_piv.TOTAL_AERIAL_BLM + dfh_res_piv.TOTAL_AERIAL_CDF + \
                                    dfh_res_piv.TOTAL_AERIAL_CNTY + dfh_res_piv.TOTAL_AERIAL_DDQ + \
                                    dfh_res_piv.TOTAL_AERIAL_DHS + dfh_res_piv.TOTAL_AERIAL_FWS + \
                                    dfh_res_piv.TOTAL_AERIAL_IA + dfh_res_piv.TOTAL_AERIAL_INTL + \
                                    dfh_res_piv.TOTAL_AERIAL_LGR + dfh_res_piv.TOTAL_AERIAL_NPS + \
                                    dfh_res_piv.TOTAL_AERIAL_OES + dfh_res_piv.TOTAL_AERIAL_ORC + \
                                    dfh_res_piv.TOTAL_AERIAL_OTHR + dfh_res_piv.TOTAL_AERIAL_PRI + \
                                    dfh_res_piv.TOTAL_AERIAL_ST + dfh_res_piv.TOTAL_AERIAL_USFS + \
                                    dfh_res_piv.TOTAL_AERIAL_WXW
    # save output file
    dfh_res_ext = dfh_res_piv[['INCIDENT_ID','REPORT_TO_DATE','TOTAL_PERSONNEL','TOTAL_AERIAL']]
    return dfh_res_ext
    
def historical2_merge_prep():
    df = pd.read_csv(os.path.join(out_dir,
                                  "IMSR_IMSR_209_INCIDENTS_{}.csv".format(hist_timespan)),
                     low_memory=False)
    df_lu = pd.read_csv(os.path.join(out_dir,
                                     'IMSR_LOOKUPS.csv'))
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    print(df.shape)
    df.drop_duplicates(inplace=True)
    print(df.shape)
    
    df = _clean_and_format_date_and_time_fields(df)
    df = _split_duplicate_incident_numbers(df)
    df = _derive_new_fields(df)
    df = _general_field_cleaning(df)
    df = _standardized_fields(df)
    df = _ks_merge_purge_duplicates(df)
    df = _latitude_longitude_updates(df)
    
    # create id xref and join data from related tables
    dfIDxref = df[['INCIDENT_NUMBER','REPORT_TO_DATE','INCIDENT_ID','FIRE_EVENT_ID']]
    dfIDxref = dfIDxref.drop_duplicates()
    
    df_str_ext = _get_str_ext(dfIDxref)
    df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE, errors='coerce')
    df_ext = pd.merge(df,df_str_ext,on=['INCIDENT_ID','REPORT_TO_DATE'],how='left')
    df_res_ext = _get_res_ext(dfIDxref)
    df_ext = pd.merge(df_ext,df_res_ext,on=['INCIDENT_ID','REPORT_TO_DATE'],how='left')
    print("Historical System 2 Merge preparation complete. {}".format(df_ext.shape))
    df_ext.to_csv(os.path.join(out_dir,
                               "IMSR_IMSR_209_INCIDENTS_{}_cleaned.csv".format(hist_timespan)))
    
