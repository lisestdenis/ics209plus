import os
import ics209util
import pandas as pd
import numpy as np
import earthpy as et

lgcy_timespan = '1999to2002'
data_dir = os.path.join(et.io.HOME, 'data')
out_dir = os.path.join(data_dir, 'out')


def _split_duplicate_incident_numbers(df):
    # set to string and set default seq number to 1
    df.EVENT_ID = df.EVENT_ID.apply(str)
    df['SEQ_NUM'] = "1"
    #df['ENAME'] = df.ENAME.astype(str).str.strip()
    
    # fix incident number errors & issues to fix joins with FOD
    df.loc[df['ENAME'] == "Kirk Complex",'EVENT_ID'] = "CA-LPF-865" 
    df.loc[df['ENAME'] == "Holser",'EVENT_ID'] = "CA-RSS-18889" # fix to join with FOD
    df.loc[df['ENAME'] == "NORTH SHORE KENAI LAKE",'EVENT_ID'] = "AK-CGF-00082"
    
    df.loc[df.EVENT_ID=='MN-MNS-','ITYPE'] = 'WF' # fix issue with hurricanes in April in MN with FIRE in name
    
    df.loc[((df['EVENT_ID'] == "CA-MVU1024") &(df['ENAME'] == "GAVILAN")),'EVENT_ID'] = "CA-MVU-1024" #add missing -
    df.loc[((df['EVENT_ID'] == "FL-FLS-") & (df['ENAME'] == "Flowers")), 'EVENT_ID'] = "FL-FLS-00029"
    df.loc[((df['EVENT_ID'] == "MT-KNF-") & (df['ENAME'] == "Troy South")), 'EVENT_ID'] = "MT-KNF-1011"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == 'Manuelitas')), 'EVENT_ID'] = "NM-4NS-139"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == 'MANUELITAS')), 'EVENT_ID'] = "NM-4NS-139"
    df.loc[((df['EVENT_ID'] == "OR-VAD-") & (df['ENAME'] == "Baker Complex")), 'EVENT_ID'] = "OR-VAD-225"
    df.loc[(df['EVENT_ID'] == '020299'),'EVENT_ID'] = 'CA-RRU-020299' # CAJALCO one sitrep incomplete
    df.loc[(df['EVENT_ID'] == '12700'),'EVENT_ID'] = 'CA-BTU-12700' # 70 Fire
    
    # split duplicates with seq#
    df.loc[((df['EVENT_ID'] == "AZ-BAR-") & (df['ENAME'] == "City Hall 2")), 'SEQ_NUM'] = "2" 
    df.loc[((df['EVENT_ID'] == "CA-RCC-") & (df['ENAME'] == "Tehama Glenn Ranger Unit")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "CA-RSS-") & (df['ENAME'] == "Goat CA LAC 99119650")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "FL-FLS-00031") & (df['ENAME'] == "Green Swamp Grove Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "FL-FLS-00031") & (df['ENAME'] == "DM Fire")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "FL-FLS-99029") & (df['ENAME'] == "Bardin")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "FL-FNF-011002") & (df['ENAME'] == "DELUTH 22")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "FL-FNF-011002") & (df['ENAME'] == "Egret 28")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "FL-FNF-011002") & (df['ENAME'] == "Shanty 32")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "LA-LAS-011008") & (df['ENAME'] == "Twin Lakes")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "MN-MNS-") & (df['ENAME'] == "Andover Fire")), 'SEQ_NUM'] = "2" #1999
    df.loc[((df['EVENT_ID'] == "MN-MNS-") & (df['ENAME'] == "Turtle Bay Complex")), 'SEQ_NUM'] = "2" #2000
    df.loc[((df['EVENT_ID'] == "MN-MNS-") & (df['ENAME'] == "St. Croix Fire")), 'SEQ_NUM'] = "3" #2000
    df.loc[((df['EVENT_ID'] == "MN-SUF-") & (df['ENAME'] == "Independence Day 99 s & rescue")), 'SEQ_NUM'] = "2" 
    df.loc[((df['EVENT_ID'] == "NJ-NJS-") & (df['ENAME'] == "Hampton Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "NM-N3S-") & (df['ENAME'] == "Iron")), 'SEQ_NUM'] = "2" #1999
    df.loc[((df['EVENT_ID'] == "NM-N3S-") & (df['ENAME'] == "Harley")), 'SEQ_NUM'] = "3" #1999
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == "CURRY RD. 3")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == "River")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == "RIVER")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == "Bell")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "NM-4NS-") & (df['ENAME'] == "BELL")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "NV-CCD-") & (df['ENAME'] == "Cottonwood")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "OK-OKS-") & (df['ENAME'] == "Round Mountain Fire")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "PA-PAS-001") & (df['ENAME'] == "HUCKLEBERRY ROAD")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "PA-PAS-003") & (df['ENAME'] == "DEHART DAM")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "SC-FMF-") & (df['ENAME'] == "Screwdriver")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "SC-FMF-") & (df['ENAME'] == "Railroad")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TN-CNF-008") & (df['ENAME'] == "River #1")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TN-CNF-010") & (df['ENAME'] == "Peter's Cemetary")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TN-CNF-011") & (df['ENAME'] == "Clarks Creek")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TN-CNF-013") & (df['ENAME'] == "Holston Mountain")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TN-GSP-001") & (df['ENAME'] == "Tunnel Fire")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00019") & (df['ENAME'] == "Naravista")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025A") & (df['ENAME'] == "SHATTLES")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025A") & (df['ENAME'] == "DOC'S")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025A") & (df['ENAME'] == "FLYING L RANCH")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025A") & (df['ENAME'] == "SPANISH GRANT")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025A") & (df['ENAME'] == "QUAIL RUN")), 'SEQ_NUM'] = "6"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025B") & (df['ENAME'] == "LIVE OAK LOOP")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025B") & (df['ENAME'] == "POSSUM KINGDOM")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025B") & (df['ENAME'] == "ROSEBUD")), 'SEQ_NUM'] = "4" 
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025B") & (df['ENAME'] == "NOLAN")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025C") & (df['ENAME'] == "MONTEITH")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025C") & (df['ENAME'] == "LOCO GRANDE")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025C") & (df['ENAME'] == "GRAFORD")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025E") & (df['ENAME'] == "CROWLEY")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025P") & (df['ENAME'] == "MILAM ROAD")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "NEWTON CIRCLE")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "Union Wells II")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "UNIION VALLEY")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "GEORGETOWN")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "LAKE BROWN")), 'SEQ_NUM'] = "6"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "POST OAK")), 'SEQ_NUM'] = "7"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "HENRIETTA")), 'SEQ_NUM'] = "8"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "JACKSBORO")), 'SEQ_NUM'] = "9"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "COCKRUM")), 'SEQ_NUM'] = "10"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "WEST NUECES")), 'SEQ_NUM'] = "11"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "GUTHRIE")), 'SEQ_NUM'] = "12"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "WILLOW CREEK")), 'SEQ_NUM'] = "13"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "Lytton Springs")), 'SEQ_NUM'] = "14"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "STEPHENVILLE")), 'SEQ_NUM'] = "15"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "Leander")), 'SEQ_NUM'] = "16"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "County Road 127")), 'SEQ_NUM'] = "17"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00025") & (df['ENAME'] == "EL RANCHO")), 'SEQ_NUM'] = "18"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "VISTULA")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "SCRUB CREEK")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "HOPE")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "ROSELAND")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "HUFFINES")), 'SEQ_NUM'] = "6"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "IRON ORE COMPLEX")), 'SEQ_NUM'] = "7"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026A") & (df['ENAME'] == "Cumby")), 'SEQ_NUM'] = "8"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026B") & (df['ENAME'] == "C & R FIRE")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026B") & (df['ENAME'] == "CHICKEN COMPLEX")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026B") & (df['ENAME'] == "LAVADA")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026B") & (df['ENAME'] == "MOORE BRANCH")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026C") & (df['ENAME'] == "BROADDUS")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026C") & (df['ENAME'] == "OAKRY")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026C") & (df['ENAME'] == "CHICKEN")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026D") & (df['ENAME'] == "PINEY GROVE")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026D") & (df['ENAME'] == "BUTTER CREEK")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026E") & (df['ENAME'] == "TURKEY")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026E") & (df['ENAME'] == "INDIAN CAMP")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026G") & (df['ENAME'] == "COOPER WHOOPS")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026L") & (df['ENAME'] == "RED TOWN")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "GATOR MARSH WEST")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "ROBERTSON ROAD")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "WESTINGHOUSE")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "SLICKEM SLOUGH")), 'SEQ_NUM'] = "5"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "FOOTBALL")), 'SEQ_NUM'] = "6"
    df.loc[((df['EVENT_ID'] == "TX-TXS-00026") & (df['ENAME'] == "JONES CREEK")), 'SEQ_NUM'] = "7"
    df.loc[((df['EVENT_ID'] == "TX-TXS-99049") & (df['ENAME'] == "Channing")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-") & (df['ENAME'] == "Earles Chapel")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-TXS-") & (df['ENAME'] == "Bragg Road")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "TX-TXS-") & (df['ENAME'] == "Hopewell Fire")), 'SEQ_NUM'] = "4"
    df.loc[((df['EVENT_ID'] == "TX-WTS-00025") & (df['ENAME'] == "KING")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-WTS-10008") & (df['ENAME'] == "RICE")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-WTS-") & (df['ENAME'] == "STAR HOLLOW")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "TX-WTS-") & (df['ENAME'] == "Cherokee Fire")), 'SEQ_NUM'] = "3"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "Hickory Ck")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "Smith Mt. Lake")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "Dog Slaughter Ridge 99")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "Little Black Mt.")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "MASON KNOB")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "VA-VAS-") & (df['ENAME'] == "CLINCH MT")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "WY-CPX-005") & (df['ENAME'] == "PONDEROSA")), 'SEQ_NUM'] = "2"
    df.loc[((df['EVENT_ID'] == "WY-RSD-") & (df['ENAME'] == "MURPHY DRAW")), 'SEQ_NUM'] = "2"
    
    return df

def _clean_and_format_date_and_time_fields(df):
    # Current Year
    df['CY'] = df['REPDATE'].astype('datetime64[ns]').dt.year
    df['REPDATE'] = pd.to_datetime(df.REPDATE)
    df['HOUR'] = 0.0
    
    # Start Year/Start Date
    df.STARTDATE = df.STARTDATE.astype(str).str.replace('1901','2001')
    df.STARTDATE = df.STARTDATE.astype(str).str.replace('1902','2002')
    df.STARTDATE = df.STARTDATE.astype(str).str.replace('1910','2001')
    df.STARTDATE = df.STARTDATE.str.replace('nan','')
    df['STARTDATE'] = pd.to_datetime(df.STARTDATE)
    df['START_YEAR'] = df['STARTDATE'].astype('datetime64[ns]').dt.year
    df.loc[df.START_YEAR.isnull(), 'START_YEAR'] = df.CY
    return df

def _derive_new_fields(df):
    # fire Event ID
    df.loc[df.ITYPE.isin(['WF','WFU','RX']),'FIRE_EVENT_ID'] = df.EVENT_ID.astype(str).str.strip() + "|" + \
               df.START_YEAR.astype(int).astype(str) + "|" + df.SEQ_NUM
    
    # POO_LATITUDE/POO_LONGITUDE decimal version to replace deg/min format
    latlongcols = ['LATDEG','LATMIN','LONGDEG','LONGMIN']
    df[latlongcols] = df[latlongcols].fillna(0)
    
    df['POO_LATITUDE'] = ics209util.dms2dd(df['LATDEG'],df['LATMIN'])
    df['POO_LONGITUDE'] = ics209util.dms2dd(df['LONGDEG'],df['LONGMIN'])
    df['POO_LONGITUDE'] = df.POO_LONGITUDE * -1.0 # adjust sign for longitude
    return df
     
def _general_field_cleaning(df):
    # Area measurement 'Acres'
    df['AREA_MEASUREMENT'] = 'Acres'
    
    # upper case incident name
    df['ENAME'] = df['ENAME'].str.upper()
    
    # remove non-numeric values from containment values
    df[['F_CONTAIN']] = df[["F_CONTAIN"]].apply(pd.to_numeric,errors='coerce')
    df.loc[df.F_CONTAIN > 100, 'F_CONTAIN'] = (df.F_CONTAIN/df.ACRES)
    
    # containment date cleaning
    df.CDATE = df.CDATE.astype(str).str.replace(r'u[nk][nk]*o*w*n*','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace('see remarks','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace(r'on*wn','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace('nr','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace('none','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace(r'no est\.?i*t*i*m*a*t*e*','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace('nec','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace(r'mon*itor','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace(r'n[\/]*[ra]*n*','',case=False)
    df.CDATE = df.CDATE.astype(str).str.replace('est ','',case=False)
    
    # remove junk values from narrative field
    df['NARRATIVE'] = df.NARRATIVE.apply(np.vectorize(ics209util.clean_narrative_text))
    
    # dispatch priority
    df.DPRIORITY = df.DPRIORITY.astype(str).str.replace('\D','')
    return df
    
def _standardized_fields(df):
    # cause 
    cm = {'H':'H','L':'L','U':'U','N':'O'}
    df['CAUSE'] = df['CAUSE'].map(cm)
    
    # team type inc mgmt org desc
    df['TEAMTYPE'] = df['TEAMTYPE'].str.upper().str.extract('([A-Z1-9])',expand=False)
    tt = {'1':'Type 1 Team','2':'Type 2 Team','3':'Type 3 Team','4':'Type 3 IC','5':'Type 4 IC','6':'Type 5 IC',
      '7':'Type 1 IC','8':'Type 2 IC','A':'FUM1','B':'FUM2','C':'Area Command','D':'Unified Command','E':'SOPL',
      'F':'FUMT'}
    df['IMT_MGMT_ORG_DESC'] = df['TEAMTYPE'].map(tt)
    
    # incident type 
    itidmap = {'EQ':9855,'FL':9856,'HM':9858,'HU':9860,'TO':9867,'WF':9851,'WFU':1,'RX':2,'SAR':9864,'OT':9925}
    df['INCTYP_IDENTIFIER'] = df['ITYPE'].map(itidmap)
    
    # state (UN_USTATE, POO_STATE_NAME)
    st_df = pd.read_csv(os.path.join(out_dir,
                                     'COMMONDATA_STATES_2014.csv'))
    st_lu = st_df[['STATE','STATE_NAME']]
    st_lu = st_lu.dropna(axis=0,how='any')
    st_lu.columns = ['UN_USTATE','POO_STATE_NAME']
    df = df.merge(st_lu, how='left')
    return df
    
def _ks_merge_purge_duplicates(df):
    # drop true duplicates in dataset
    df = df.drop_duplicates()
    
    # read in KS dataset and drop duplicates
    df_short = pd.read_excel(os.path.join(data_dir,
                                          'raw',
                                          'excel',
                                          'Short1999to2013v2.xlsx'))
    df_short['INCIDENT_NAME'] = df_short['INCIDENT_NAME'].astype(str)
    df_short['INCIDENT_NUMBER'] = df_short['INCIDENT_NUMBER'].astype(str)
    df_short = df_short.drop_duplicates()
    
    # prepare short columns for merge
    df_short['HOUR'] = df_short['HOUR'].fillna(0.0)
    df_short['REPORT_DATE'] = pd.to_datetime(df_short.REPORT_DATE)
    short_cols = df_short[['REPORT_DATE','HOUR','INCIDENT_NUMBER','INCIDENT_ID_KS','KS_COMPLEX_NAME',
                           'INCIDENT_NAME_CORRECTED','INCIDENT_NUMBER_CORRECTED','START_DATE_CORRECTED']]
    short_cols.columns = ['REPDATE','HOUR','EVENT_ID','INCIDENT_ID','COMPLEX_NAME','INCIDENT_NAME_CORRECTED',
                          'INCIDENT_NUMBER_CORRECTED','DISCOVERY_DATE_CORRECTED']
    short_cols['INCIDENT_ID'] = short_cols.INCIDENT_ID.astype(str).str.strip().str.upper()
    # merge
    df = pd.merge(df, short_cols, on=['EVENT_ID','REPDATE','HOUR'], how='left')
    
    # Save file with deleted records:
    df_notinKS = df.loc[df.INCIDENT_ID.isnull() & df.ITYPE.isin(['WF','WFU'])]
    df_notinKS.to_csv('../../data/out/ics209_sitreps_deleted_hist1_{}.csv'.format(lgcy_timespan)) 
    # Save file merged with KS records as cleanedKS version
    df = df.loc[~df.INCIDENT_ID.isnull() | ~df.ITYPE.isin(['WF','WFU'])]
    df.loc[~df.ITYPE.isin(['WF','WFU']),'INCIDENT_ID'] = df.START_YEAR.astype(int).astype(str) + '_' \
                                    + df.EVENT_ID.astype(str).str.strip() + '_' + \
                                    df.ENAME.astype(str).str.strip()
    # set complex flag
    df.loc[(df.COMPLEX_NAME.notnull() & df.ITYPE.isin(['WF','WFU'])),'COMPLEX'] = True
    df.loc[(df.COMPLEX_NAME.isnull() & df.ITYPE.isin(['WF','WFU'])),'COMPLEX'] = False
    return df
    
def _latitude_longitude_updates(df):
    leg_loc = pd.read_csv(os.path.join(data_dir,
                                       'raw',
                                       'latlong_clean',
                                       'legacy_cleaned_ll-fod.csv'))
    df = df.merge(leg_loc, on=['FIRE_EVENT_ID'],how='left')
    # Set the Update Flag
    df.loc[df.lat_c.notnull(),'LL_UPDATE'] = True
    
    # Case #1: Update using estimated lat/long value 
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'POO_LATITUDE'] = df.lat_c # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c != 0)),'POO_LONGITUDE'] = df.long_c # set longitude to nan
    # Case #2: Update FOD lat/long
    df.loc[df.FOD_LATITUDE.notnull(), 'POO_LATITUDE'] = df.FOD_LATITUDE
    df.loc[df.FOD_LONGITUDE.notnull(), 'POO_LONGITUDE'] = df.FOD_LATITUDE
    # Case #3: Unable to fix so set to null
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'POO_LATITUDE'] = np.nan # set latitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'POO_LONGITUDE'] = np.nan # set longitude to nan
    df.loc[((df.lat_c.notnull()) & (df.lat_c == 0)),'LL_CONFIDENCE'] = 'N'
    
    return df

def _get_str_ext(uid_xref):
    # Read in structure tables
    dfl_str = pd.read_csv('../../data/out/IMSR_INCIDENT_STRUCTURES_{}.csv'.format(lgcy_timespan))
    dfl_str = dfl_str.loc[:, ~dfl_str.columns.str.contains('^Unnamed')]
    
    # Fix mis-matched EVENT_IDs prior to UID merge
    dfl_str.loc[(dfl_str['II_EVENT_ID'] == '20299'),'II_EVENT_ID'] = 'CA-RRU-020299' # CAJALCO one sitrep incomplete
    dfl_str.loc[(dfl_str['II_EVENT_ID'] == '12700'),'II_EVENT_ID'] = 'CA-BTU-12700' # 70 Fire
   
    # merge in unique ID
    dfl_str['II_REPDATE'] = pd.to_datetime(dfl_str.II_REPDATE, errors='coerce')
    dfl_str = dfl_str.merge(uid_xref,on=['II_EVENT_ID','II_REPDATE'],how='left')
 
    # ignore records where incident id is null (~10 records)
    dfl_str = dfl_str.loc[dfl_str.INCIDENT_ID.notnull()]
    
    dfl_str_piv = dfl_str.pivot_table(index = ['INCIDENT_ID','II_REPDATE'], columns='STYPE',values=['DCOUNT','TCOUNT'])
    # Clean up the column names
    dfl_str_piv.columns = ["_".join((i,j)) for i,j in dfl_str_piv.columns]
    # Set columns to conform to historical naming convention
    dfl_str_piv.columns = ['STR_DESTROYED_COMM','STR_DESTROYED_OUTB','STR_DESTROYED_PRIM','STR_DESTROYED_SEAS',\
                            'STR_THREATENED_COMM','STR_THREATENED_OUTB','STR_THREATENED_PRIM','STR_THREATENED_SEAS']
    
    # prep for forward fill
    dfl_str_piv = dfl_str_piv.fillna(0)
    dfl_str_piv.reset_index(inplace=True)
    #Forward fill missing values for destroyed columns
    rows = dfl_str_piv.shape[0]
    for i in range(0,rows-1):
        # Check to see if current rows belong to same incident
        if dfl_str_piv.iloc[i+1].INCIDENT_ID == dfl_str_piv.iloc[i].INCIDENT_ID:
            if dfl_str_piv.iloc[i+1].STR_DESTROYED_COMM == 0 and dfl_str_piv.iloc[i].STR_DESTROYED_COMM > 0:
                dfl_str_piv.loc[i+1,'STR_DESTROYED_COMM'] = dfl_str_piv.loc[i,'STR_DESTROYED_COMM'] #propagate prev value
            if dfl_str_piv.iloc[i+1].STR_DESTROYED_OUTB == 0 and dfl_str_piv.iloc[i].STR_DESTROYED_OUTB > 0:
                dfl_str_piv.loc[i+1,'STR_DESTROYED_OUTB'] = dfl_str_piv.loc[i,'STR_DESTROYED_OUTB'] #propagate prev value
            if dfl_str_piv.iloc[i+1].STR_DESTROYED_PRIM == 0 and dfl_str_piv.iloc[i].STR_DESTROYED_PRIM > 0:
                dfl_str_piv.loc[i+1,'STR_DESTROYED_PRIM'] = dfl_str_piv.loc[i,'STR_DESTROYED_PRIM'] #propagate prev value
            if dfl_str_piv.iloc[i+1].STR_DESTROYED_SEAS == 0 and dfl_str_piv.iloc[i].STR_DESTROYED_SEAS > 0:
                dfl_str_piv.loc[i+1,'STR_DESTROYED_SEAS'] = dfl_str_piv.loc[i,'STR_DESTROYED_SEAS'] #propagate prev value
     
    # create total fields
    dfl_str_piv['STR_DESTROYED'] = dfl_str_piv.STR_DESTROYED_COMM + dfl_str_piv.STR_DESTROYED_OUTB +\
                                   dfl_str_piv.STR_DESTROYED_PRIM + dfl_str_piv.STR_DESTROYED_SEAS
    dfl_str_piv['STR_THREATENED'] = dfl_str_piv.STR_THREATENED_COMM + dfl_str_piv.STR_THREATENED_OUTB +\
                                    dfl_str_piv.STR_THREATENED_PRIM + dfl_str_piv.STR_THREATENED_SEAS
    dfl_str_piv['STR_DESTROYED_RES'] = dfl_str_piv.STR_DESTROYED_PRIM + dfl_str_piv.STR_DESTROYED_SEAS
    dfl_str_piv['STR_THREATENED_RES'] = dfl_str_piv.STR_THREATENED_PRIM + dfl_str_piv.STR_THREATENED_SEAS
    
    # save output files
    dfl_str_piv.to_csv(os.path.join(out_dir,
                                    "IMSR_INCIDENT_STRUCTURES_{}_pivot.csv".format(lgcy_timespan)))
    df_str_ext = dfl_str_piv[['INCIDENT_ID','II_REPDATE','STR_DESTROYED','STR_THREATENED',\
                             'STR_DESTROYED_RES','STR_THREATENED_RES',\
                             'STR_DESTROYED_COMM','STR_THREATENED_COMM']]
    df_str_ext.columns = df_str_ext.columns.str.replace('II_REPDATE','REPDATE')
    return df_str_ext
    
def _get_res_ext(uid_xref):
    # personnel is in sitrep
    dfl_res = pd.read_csv(os.path.join(out_dir,
                                       "IMSR_INCIDENT_RESOURCES_{}.csv".format(lgcy_timespan)))
    dfl_res = dfl_res.loc[:, ~dfl_res.columns.str.contains('^Unnamed')]
    
    # fix errors
    dfl_res.loc[(dfl_res['II_EVENT_ID'] == '20299'),'II_EVENT_ID'] = 'CA-RRU-020299' # CAJALCO one sitrep incomplete
    dfl_res.loc[(dfl_res['II_EVENT_ID'] == '12700'),'II_EVENT_ID'] = 'CA-BTU-12700' # 70 Fire
    
    dfl_res['II_REPDATE'] = pd.to_datetime(dfl_res.II_REPDATE, errors='coerce')
    dfl_res = dfl_res.merge(uid_xref,on=['II_EVENT_ID','II_REPDATE'],how='left')
    dfl_res = dfl_res.loc[dfl_res.INCIDENT_ID.notnull()] # trim out rows where there is no INCIDENT_ID
    
    dfl_res['TOTAL_HEL'] = dfl_res.HEL1.fillna(0) + dfl_res.HEL2.fillna(0) + dfl_res.HEL3.fillna(0)
    dflrpiv = dfl_res.pivot_table(index = ['INCIDENT_ID','II_REPDATE'],columns='AG_AID',values=['TOTAL_HEL'])
    dflrpiv.columns = ["_".join((i,j)) for i,j in dflrpiv.columns]
    dflrpiv['TOTAL_AERIAL'] = dflrpiv.TOTAL_HEL_BIA.fillna(0) + dflrpiv.TOTAL_HEL_BLM.fillna(0) + \
    dflrpiv.TOTAL_HEL_CDF.fillna(0) + dflrpiv.TOTAL_HEL_CNTY.fillna(0) + dflrpiv.TOTAL_HEL_DDQ.fillna(0) + \
    dflrpiv.TOTAL_HEL_FWS.fillna(0) + dflrpiv.TOTAL_HEL_IA.fillna(0) + dflrpiv.TOTAL_HEL_LGR.fillna(0) + \
    dflrpiv.TOTAL_HEL_NPS.fillna(0) + dflrpiv.TOTAL_HEL_OES.fillna(0) + dflrpiv.TOTAL_HEL_OTHR.fillna(0) + \
    dflrpiv.TOTAL_HEL_PRI.fillna(0) + dflrpiv.TOTAL_HEL_ST.fillna(0) + dflrpiv.TOTAL_HEL_USFS.fillna(0) + \
    dflrpiv.TOTAL_HEL_WXW.fillna(0)
    dflrpiv.reset_index(inplace=True)
    dflr_ext = dflrpiv[['INCIDENT_ID','II_REPDATE','TOTAL_AERIAL']]
    dflr_ext.columns = dflr_ext.columns.str.replace('II_REPDATE','REPDATE')
    return dflr_ext

def historical1_merge_prep():
    df = pd.read_csv(os.path.join(out_dir, 
                                  "IMSR_INCIDENT_INFORMATIONS_{}.csv".format(lgcy_timespan)))
    lu_df = pd.read_csv(os.path.join(out_dir,
                                     "IMSR_LOOKUPS.csv"))
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    print(df.shape)
    df.drop_duplicates(inplace=True)
    print(df.shape)
    df = _split_duplicate_incident_numbers(df)
    df = _clean_and_format_date_and_time_fields(df)
    df = _derive_new_fields(df)
    df = _general_field_cleaning(df)
    df = _standardized_fields(df)
    df = _ks_merge_purge_duplicates(df)
    df = _latitude_longitude_updates(df)
    
    # create id xref and join data from related tables
    dfIDxref = df[['EVENT_ID','REPDATE','INCIDENT_ID','FIRE_EVENT_ID']]
    dfIDxref = dfIDxref.drop_duplicates()
    dfIDxref.columns = ['II_EVENT_ID','II_REPDATE','INCIDENT_ID','FIRE_EVENT_ID']
    df_str_ext = _get_str_ext(dfIDxref)
    df_ext = pd.merge(df,df_str_ext,on=['INCIDENT_ID','REPDATE'],how='left')
    df_res_ext = _get_res_ext(dfIDxref)
    df_ext = pd.merge(df_ext,df_res_ext,on=['INCIDENT_ID','REPDATE'],how='left')

    print("Historical System 1 merge preparation complete {}".format(df_ext.shape))
    
    df_ext.to_csv(os.path.join(out_dir,
                               "IMSR_INCIDENT_INFORMATIONS_{}_cleaned.csv".format(lgcy_timespan)))
    
