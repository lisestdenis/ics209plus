import os
import sys
from datetime import timedelta
import re
import pandas as pd
import numpy as np
import ics209util
import earthpy as et
from scipy import stats
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
import seaborn as sns

def is_sitrep(year, file_suffix):
    """
    is_sitrep determines if current input file is_siterp based on year + file ending
    
    Parameters:
        year: current year determines file naming convention
        file_suffix: current file suffix

    Returns: True/False
    """
    if year < 2014:
        if file_suffix in ['S','S_T','INFORMATIONS'] :
            return True
    else: # 2014+
        if file_suffix == '_209_REPORTS' :
            return True
    return False


def clean_narrative_text(narr_text):
    """
    clean_narrative_text strips placeholder values equivalent to empty strings (e.g. None, Same, null...) 
    from narrative text fields
    
    Parameters:
        narrText string to be cleaned
    
    Returns:
        version of the string without null values or np.nan for null strings
    """
    narr_text = str(narr_text)
    # lowercase before cleaning with reg exp
    narr_text = narr_text.strip().lower()
    # replace the word 'null'
    narr_text = narr_text.replace('null','')
    # replace the value "n/a"
    narr_text = narr_text.replace("n/a","") 
    # delete variations on "none..."
    narr_text = re.sub('(there are)*\s* none[ a-z\.]*','',narr_text) 
    # delete varations on "no ..."
    narr_text = re.sub('(there are)*\s*no [a-z \.,\-]*','',narr_text) 
    # delete varations on "no ..."
    narr_text = re.sub('(same[a-z \.,\-]*)','',narr_text) 
    
    # strip before final test
    narr_text = narr_text.strip()
    if narr_text:
        return narr_text
    else:
        return np.nan   

def remove_problematic_chars(year, df, legacyFlag):
    """
    Remove hidden characters from free-form text fields in the sitrep records within the legacy fields where it was a problem.
    The files originate from IBM Cognos and the free-form text fields can contain the '\r' (carriage return) character. 
    Newline characters are also removed as part of the cleaning.

    Parameters:
    year : the current year. 2001-2013 are part of historical tables, 2014+ are part of new system. The fields
            on the 209 form change from year to year, so year is used to identify which fields need cleaning.
    df: the current data frame being processed
    legacyFlag: Boolean True if from earliest legacy system
    
    Returns:
    df: cleaned df
    """
    
    replexp = {'[\n\r\t\,]' : ' '}
    if year < 2014: # historical
        if legacyFlag :
            df['NARRATIVE'] = df['NARRATIVE'].astype(str).str.replace('\n','')
            df['NARRATIVE'] = df['NARRATIVE'].astype(str).str.replace('\r','')
        else:
            df.FUELS = df.FUELS.replace(replexp, regex=True)
            df.PLANNED_ACTIONS = df.PLANNED_ACTIONS.replace(replexp, regex=True)
            df.LOCATION = df.LOCATION.replace(replexp, regex=True)
            df.OBS_FIRE_BEHAVE = df.OBS_FIRE_BEHAVE.replace(replexp, regex=True)
            df.SIG_EVENT = df.SIG_EVENT.replace(replexp, regex=True)
            df.COOP_AGENCIES = df.COOP_AGENCIES.replace(replexp, regex=True)
            df.CRITICAL_RES = df.CRITICAL_RES.replace(replexp, regex=True)
            df.PROJECTED_MOVEMENT = df.PROJECTED_MOVEMENT.replace(replexp, regex=True)
            df.MAJOR_PROBLEMS = df.MAJOR_PROBLEMS.replace(replexp, regex=True)
            df.TARGETS_MET = df.TARGETS_MET.replace(replexp, regex=True)
            df.REMARKS = df.REMARKS.replace(replexp, regex=True)
            if year > 2001:
                df.GACC_REMARKS = df.GACC_REMARKS.replace(replexp, regex=True)
                df.GACC_SIG_EVENT = df.GACC_SIG_EVENT.replace(replexp, regex=True)
                df.GACC_OBS_FIRE_BEHAVE = df.GACC_OBS_FIRE_BEHAVE.replace(replexp, regex=True)
                df.GACC_PLANNED_ACTIONS = df.GACC_PLANNED_ACTIONS.replace(replexp, regex=True)
            if year > 2002:
                df.COMMUNITIES_THREATENED_12 = df.COMMUNITIES_THREATENED_12.replace(replexp, regex=True)
                df.COMMUNITIES_THREATENED_24 = df.COMMUNITIES_THREATENED_24.replace(replexp, regex=True)
                df.COMMUNITIES_THREATENED_48 = df.COMMUNITIES_THREATENED_48.replace(replexp, regex=True)
                df.COMMUNITIES_THREATENED_72 = df.COMMUNITIES_THREATENED_72.replace(replexp, regex=True)
            if year > 2006:
                df.CRITICAL_RES24 = df.CRITICAL_RES24.replace(replexp, regex=True)
                df.CRITICAL_RES48 = df.CRITICAL_RES48.replace(replexp, regex=True)
                df.CRITICAL_RES72 = df.CRITICAL_RES72.replace(replexp, regex=True)
                df.PROJECTED_MOVEMENT24 = df.PROJECTED_MOVEMENT24.replace(replexp, regex=True)
                df.PROJECTED_MOVEMENT48 = df.PROJECTED_MOVEMENT48.replace(replexp, regex=True)
                df.PROJECTED_MOVEMENT72 = df.PROJECTED_MOVEMENT72.replace(replexp, regex=True)
                #df.RISK_ASSESSMENT = df.RISK_ASSESSMENT.replace({'[\n\r]': ' '}, regex=True)
            if year < 2008:
                df.RES_THREAT = df.RES_THREAT.replace(replexp, regex=True)
                df.RES_BENEFITS = df.RES_BENEFITS.replace(replexp, regex=True)
    else:
        df.INCIDENT_COMMANDERS_NARR = df.INCIDENT_COMMANDERS_NARR.replace(replexp, regex=True)
        df.SIGNIF_EVENTS_SUMMARY = df.SIGNIF_EVENTS_SUMMARY.replace(replexp, regex=True)
        df.REMARKS = df.REMARKS.replace(replexp, regex=True)
        #df.ADDTNL_COOP_ASSIST_ORG_NARR = df.ADDTNL_COOP_ASSIST_ORG_NARR.replace(replexp, regex=True)
        df.WEATHER_CONCERNS_NARR = df.WEATHER_CONCERNS_NARR.replace(replexp, regex=True)
        df.STRATEGIC_DISCUSSION = df.STRATEGIC_DISCUSSION.replace(replexp, regex=True)
        #df.ELEC_GEOSP_DATA_INCL = df.ELEC_GEOSP_DATA_INCL.replace(replexp, regex=True)
        df.CRIT_RES_NEEDS_12 = df.CRIT_RES_NEEDS_12.replace(replexp, regex=True)
        df.CRIT_RES_NEEDS_24 = df.CRIT_RES_NEEDS_24.replace(replexp, regex=True)
        df.CRIT_RES_NEEDS_48 = df.CRIT_RES_NEEDS_48.replace(replexp, regex=True)
        df.CRIT_RES_NEEDS_72 = df.CRIT_RES_NEEDS_72.replace(replexp, regex=True)
        df.CRIT_RES_NEEDS_GT72 = df.CRIT_RES_NEEDS_GT72.replace(replexp, regex=True)
        df.PLANNED_ACTIONS = df.PLANNED_ACTIONS.replace(replexp, regex=True) 
        df.PROJECTED_ACTIVITY_12 = df.PROJECTED_ACTIVITY_12.replace(replexp, regex=True)
        df.PROJECTED_ACTIVITY_24 = df.PROJECTED_ACTIVITY_24.replace(replexp, regex=True)
        df.PROJECTED_ACTIVITY_48 = df.PROJECTED_ACTIVITY_48.replace(replexp, regex=True)
        df.PROJECTED_ACTIVITY_72 = df.PROJECTED_ACTIVITY_72.replace(replexp, regex=True)
        df.PROJECTED_ACTIVITY_GT72 = df.PROJECTED_ACTIVITY_GT72.replace(replexp, regex=True)
        df.LIFE_SAFETY_HEALTH_STATUS_NARR = df.LIFE_SAFETY_HEALTH_STATUS_NARR.replace(replexp, regex=True)
        #df.DAMAGE_ASSESSMENT_INFO = df.DAMAGE_ASSESSMENT_INFO.replace(replexp, regex=True)
        df.STRATEGIC_OBJECTIVES = df.STRATEGIC_OBJECTIVES.replace(replexp, regex=True)
        df.CURRENT_THREAT_12 = df.CURRENT_THREAT_12.replace(replexp, regex=True)
        df.CURRENT_THREAT_24 = df.CURRENT_THREAT_24.replace(replexp, regex=True)
        df.CURRENT_THREAT_48 = df.CURRENT_THREAT_48.replace(replexp, regex=True)
        df.CURRENT_THREAT_72 = df.CURRENT_THREAT_72.replace(replexp, regex=True)
        df.CURRENT_THREAT_GT72 = df.CURRENT_THREAT_GT72.replace(replexp, regex=True)
        df.HAZARDS_MATLS_INVOLVMENT_NARR = df.HAZARDS_MATLS_INVOLVMENT_NARR.replace(replexp, regex=True) 
        df.COMPLEXITY_LEVEL_NARR = df.COMPLEXITY_LEVEL_NARR.replace(replexp, regex=True)
        
    return(df)


def reformatTimestamp(df,file_suffix):
    if file_suffix == "_209_REPORTS":
        df['DISCOVERY_DATE'] = pd.to_datetime(df.DISCOVERY_DATE)
        df['APPROVED_DATE'] = pd.to_datetime(df.APPROVED_DATE)
        df['CREATED_DATE'] = pd.to_datetime(df.CREATED_DATE)
        df['LAST_MODIFIED_DATE'] = pd.to_datetime(df.LAST_MODIFIED_DATE)
        df['PROJ_SIG_RES_DEMOB_START_DATE'] = pd.to_datetime(df.PROJ_SIG_RES_DEMOB_START_DATE)
        df['REPORT_TO_DATE'] = pd.to_datetime(df.REPORT_TO_DATE)
        df['REPORT_FROM_DATE'] = pd.to_datetime(df.REPORT_FROM_DATE)
        df['SUBMITTED_DATE'] = pd.to_datetime(df.SUBMITTED_DATE)
        
    return df

def reformatMSTimestamp(df,file_suffix):
    epochStart = pd.Timestamp(1899,12,30,0)
    if file_suffix == "_209_REPORTS":
        df.loc[df['DISCOVERY_DATE'].notnull(),'DISCOVERY_DATE'] = epochStart + \
                                                                  pd.to_timedelta(np.floor(df.DISCOVERY_DATE * 86400),'s')
        df.loc[df['APPROVED_DATE'].notnull(),'APPROVED_DATE'] = epochStart + \
                                                                pd.to_timedelta(np.floor(df.APPROVED_DATE * 86400),'s')
        df.loc[df['CREATED_DATE'].notnull(),'CREATED_DATE'] = epochStart + \
                                                              pd.to_timedelta(np.floor(df.CREATED_DATE * 86400),'s')
        df.loc[df['LAST_MODIFIED_DATE'].notnull(),'LAST_MODIFIED_DATE'] = epochStart + \
                                                              pd.to_timedelta(np.floor(df.LAST_MODIFIED_DATE * 86400),'s')
        df.loc[df['PREPARED_DATE'].notnull(),'PREPARED_DATE'] = epochStart + \
                                                                pd.to_timedelta(np.floor(df.PREPARED_DATE * 86400),'s')
        # The only date not in MSTimestamp format. Not sure why!
        df['PROJ_SIG_RES_DEMOB_START_DATE'] = pd.to_datetime(df.PROJ_SIG_RES_DEMOB_START_DATE)
        df.loc[df['REPORT_TO_DATE'].notnull(),'REPORT_TO_DATE'] = epochStart + \
                                                                  pd.to_timedelta(np.floor(df.REPORT_TO_DATE * 86400),'s')
        df.loc[df['REPORT_FROM_DATE'].notnull(),'REPORT_FROM_DATE'] = epochStart + \
                                                                      pd.to_timedelta(np.floor(df.REPORT_FROM_DATE * 86400),'s')
        df.loc[df['SUBMITTED_DATE'].notnull(),'SUBMITTED_DATE'] = epochStart + \
                                                                  pd.to_timedelta(np.floor(df.SUBMITTED_DATE * 86400),'s')
    return df

def dms2dd(d, m, s=[]):
    """
    dms2dd - Converts degrees, minutes and optionally seconds to dd. 
    This function can take single values or an array/dataframe of values.
    
    ARGS:
        d (int) or (float): degrees which must be in or convertable as a float
        m (int): minutes
        s (int): (Optional) seconds
    
    RETURNS:
        dd (float): decimal degrees (dd)
        
    EXAMPLE:
    >>> dd = dms2dd(d=40, m=42, s=0)
    40.7
    
    """

    #Make sure the length of arrays for d and m are the same
    try:
         if np.not_equal(len(d), len(m)):
                raise ValueError
    except ValueError as valerr:
        print('ERROR: d and m are different lengths!')
        return 0
        
    #Check to see if secomds(s) is passed in if not then create an array 's'
    #which is the same size as d. 
    #If seconds(s) is passed in make sure its the same length as d
    if len(s)==0:
        s = np.zeros(len(d))
    elif len(s) > 0:
        try:
         if np.not_equal(len(d), len(s)):
                raise ValueError
        except ValueError as valerr:
            print('ERROR: d and s are different lengths!')
            return 0
        
    #Make sure d can be cast as a float if not fail
    if type(d[0]) != 'float':
        try:
            e = float(d[0])
        except:
            print('\nERROR: Can not convert %s to float!' %(type(d)))
            return 0
        
    #Declare and init the return value 'dd'
    dd = np.zeros((0,len(d)))
    
    #Replace NaN's with 0's. I'm doing this so data with NaNs are still easily plotable.
    d[np.isnan(d)] = 0
    m[np.isnan(m)] = 0
    s[np.isnan(s)] = 0
    
    #Loop through the arrays and calculate dd for each value set
    for i in range(0, len(d)):
        dd = np.append(dd, d[i]+float(m[i])/60. + float(s[i])/3600.)
    
    #Return dd
    return dd

def unique_members(string, delimeter = "|"):
    """
    Find unique elements in a delimited string
    args: 
        string - a delimed string (e.g., "a|b|b")
        delimeter - a delimeter in the string (e.g., "|")
    returns: 
        a string with only unique members and the same delimeter
        (e.g., "a|b")
    """
    elements_in_string = string.split(delimeter)
    unique_elements = list(set(elements_in_string))
    non_na_elements = [x for x in unique_elements if str(x) != 'nan']
    return delimeter.join(non_na_elements)

def combine_text_fields(df, *args):
    """
    Combine text fields in a pandas data frame
    args: 
        df - a data frame
        *args - any number of column names
    returns: 
        a pipe delimited text field w/ unique entries f\om the columns
        
    example usage: 
        combine_text_fields(df, "column1", "column2")
    """
    columns_to_combine = [a for a in args]
    df_subset = df.loc[:, columns_to_combine]
    parsed_df_subset = df_subset.apply(np.vectorize(clean_narrative_text))
    combined_field = parsed_df_subset.apply(lambda x: '|'.join(x), axis = 1)
    return combined_field.apply(unique_members)

def get_largest_fod_rec(fod_list):
    
    print(fod_list)
    print(type(fod_list))
    lrg_ptr = fod_list[0]
    print(lrg_ptr)
    lrg_ptr = eval(lrg_ptr)
    lrg_size = lrg_ptr.get("SIZE")
    if len(fod_list) > 1:
        for j in range(0, len(fod_list)-1):
            curr_ptr = eval(fod_list[j])
            curr_size = curr_ptr.get("SIZE")
            if curr_size > lrg_size:
                lrg_ptr = curr_ptr
                lrg_size = curr_size 
    return lrg_ptr

def print_report_block(block_text,*args,**kwargs):
    """
    print_report_block prints reporting block with text, optional date (using rpt_date keyword argument) and
    incident identifier (using id keyword argument)
    
    Parameters:
        block_text: text description to be centered in report block

    Returns: none
    """
    if 'rpt_date' in kwargs:
        rpt_date = kwargs.get("rpt_date")
    else:
        rpt_date = ''
        
    if 'id' in kwargs:
        id_str = kwargs.get("id")
    else:
        id_str = ''
    block_text = block_text
    print("\n=======================================================================================================")
    print("{} {} {}".format(block_text, rpt_date, id_str).center(100," "))
    print("=======================================================================================================")
    
    
def print_incident_summary(row):
    """
    print_incident_summary prints all of elements in the incident summary record for debugging/analysis purposes
    
    Parameters:
    row: row from incident summary table
    
    Returns: none
    """
    
    print_report_block("INCIDENT_SUMMARY",id=row['INCIDENT_ID'].to_string(index=False))
    print("INCIDENT_ID: {}\t\tType: {}  Cause: {}\t#Sitreps: {}".format(
                                                      (row['INCIDENT_ID']).to_string(index=False),
                                                      (row['INCTYP_ABBREVIATION']).to_string(index=False),
                                                      (row['CAUSE']).to_string(index=False),
                                                      (row['INC_MGMT_NUM_SITREPS']).to_string(index=False)))
    print("Inc Descr: {}\t\tINC_IDENTIFIER:{}".format((row['INCIDENT_DESCRIPTION']).to_string(index=False),
                                                        (row['INC_IDENTIFIER']).to_string(index=False)))
    print("Fuel Model: {}\n".format((row['FUEL_MODEL']).to_string(index=False)))
    
    print("Final Acres:{}\tDiscovery Date: {}\t\tDOY: {}".format((row['FINAL_ACRES']).to_string(index=False),
                                                    (row['DISCOVERY_DATE']).to_string(index=False),
                                                    (row['DISCOVERY_DOY']).to_string(index=False)))
    print("Max FSR: {} \tCessation Date: {}\tCESS DOY: {}".format(
                                                (row['WF_MAX_FSR']).to_string(index=False), 
                                                (row['WF_CESSATION_DATE']).to_string(index=False),
                                                (row['WF_CESSATION_DOY']).to_string(index=False)))
    print("Growth Dur: {}\tMax Growth Date:{}\tMax Growth DOY: {}".format(
                                                (row['WF_GROWTH_DURATION']).to_string(index=False),
                                                (row['WF_MAX_GROWTH_DATE']).to_string(index=False),
                                                (row['WF_MAX_GROWTH_DOY']).to_string(index=False)))

    print("Exp Cont: {}\tFinal Rep Date: {}\t\tProj Cost: {}".format(
                                               (row['EXPECTED_CONTAINMENT_DATE']).to_string(index=False),
                                               (row['FINAL_REPORT_DATE']).to_string(index=False),
                                               (row['PROJECTED_FINAL_IM_COST']).to_string(index=False)))
    
    print("\nLOCATION:")
    print("STATE: {}\t\tLAT/LON: {},{}\t\tLL Conf: {}\t\t LLUPdate?{}".format((row['POO_STATE']).to_string(index=False),
                                 (row['POO_LATITUDE']).to_string(index=False),
                                 (row['POO_LONGITUDE']).to_string(index=False),
                                 (row['POO_LONGITUDE']).to_string(index=False),
                                 (row['LL_CONFIDENCE']).to_string(index=False),
                                 (row['LL_UPDATE']).to_string(index=False)))
    print("City: {}\t\tCounty: {}\t\t\tLocation Descr: {}".format((row['POO_CITY']).to_string(index=False),
                            (row['POO_COUNTY']).to_string(index=False),
                            (row['POO_SHORT_LOCATION_DESC']).to_string(index=False)))
    
    print("\nCAS/INJ/EVAC:")
    print("Pub Fatalities: {}\tInjuries: {}\t\t".format((row['FATALITIES']).to_string(index=False),
                                                          (row['INJURIES_TOTAL']).to_string(index=False)))
    
    print("\nRESOURCES:\tTOTAL\tPEAK\t PEAK DATE\t\tPEAK DOY")
    print("------------------------------------------------------------------")
    print("Personnel\t{}\t{}\t{}\t{}".format((row['TOTAL_PERSONNEL_SUM']).to_string(index=False),
                                        (row['WF_PEAK_PERSONNEL']).to_string(index=False),
                                        (row['WF_PEAK_PERSONNEL_DATE']).to_string(index=False),
                                        (row['WF_PEAK_PERSONNEL_DOY']).to_string(index=False)))
    print("Aerial\t\t{}\t{}\t{}\t{}".format((row['TOTAL_AERIAL_SUM']).to_string(index=False),
                                        (row['WF_PEAK_AERIAL']).to_string(index=False),
                                        (row['WF_PEAK_AERIAL_DATE']).to_string(index=False),
                                        (row['WF_PEAK_AERIAL_DOY']).to_string(index=False)))
    
    print("\nSTRUCTURES:\tTOTAL\tRESIDENTIAL\tCOMMERCIAL")
    print("---------------------------------------------------")
    print("Damaged:\t{}\t{}\t\t{}".format((row['STR_DAMAGED_TOTAL']).to_string(index=False),
                                        (row['STR_DAMAGED_RES_TOTAL']).to_string(index=False),
                                        (row['STR_DAMAGED_COMM_TOTAL']).to_string(index=False)))
    print("Destroyed:\t{}\t{}\t\t{}".format((row['STR_DESTROYED_TOTAL']).to_string(index=False),
                                        (row['STR_DESTROYED_RES_TOTAL']).to_string(index=False),
                                        (row['STR_DESTROYED_COMM_TOTAL']).to_string(index=False)))
    print("Threat(max): \t{}\t{}\t\t{}".format((row['STR_THREATENED_MAX']).to_string(index=False),
                                        (row['STR_THREATENED_RES_MAX']).to_string(index=False),
                                        (row['STR_THREATENED_COMM_MAX']).to_string(index=False)))
    
    print_report_block("INCIDENT_SUMMARY",id=row['INCIDENT_ID'].to_string(index=False))
    
def print_sitrep(row):
    """
    print_sitrep prints all elements of the sitrep wildfire sitreps table for debugging/analysis
    
    Parameters: row from sitrep table
    
    Returns: none
    """
    print_report_block("SITUATION REPORT",rpt_date=row.REPORT_TO_DATE)
    
    print("INCIDENT ID: {}\tFireEventID: {}\tDesc:{}".format(row.INCIDENT_ID,
                                                        row.FIRE_EVENT_ID,
                                                        row.INCIDENT_DESCRIPTION))
    print("Type: {}\tCause: {}\tReport Date: {}".format(row.INCTYP_ABBREVIATION,
                                                        row.CAUSE,
                                                        row.REPORT_TO_DATE))
    print("INC209R: {} Acres: {}({})\tDisc Date: {}({})\tEst Cost: {}\t".format(
                                   row.INC209R_IDENTIFIER,
                                   row.ACRES,
                                   row.NEW_ACRES,
                                   row.DISCOVERY_DATE,
                                   row.DISCOVERY_DOY,
                                   row.EST_IM_COST_TO_DATE,
                                   ))
    print("Exp Contain: {}\t%Final Size {}\t".format(row.EXPECTED_CONTAINMENT_DATE,
                                                  row.MAX_FIRE_PCT_FINAL_SIZE))
    print("\nLOCATION:")
    print("Lat/Lon: ({},{}) County: {}\tCounty Code: {}\tLoc Descr: {}\t".format(row.POO_LATITUDE,
                                                                    row.POO_LONGITUDE,
                                                                    row.POO_CITY,
                                                                    row.POO_STATE,
                                                                    row.POO_COUNTY,
                                                                    row.POO_SHORT_LOCATION_DESC
                                                                    ))
    
    print("LL Conf: {}\tLL Update {}".format(row.LL_CONFIDENCE,
                                                              row.LL_UPDATE))
    print("NGR Zone {} NGR XCoord {}\tNGR YCoord {}".format(row.POO_US_NGR_ZONE,
                                                              row.POO_US_NGR_XCOORD,
                                                              row.POO_US_NGR_XCOORD))
    print("UTM Zone {}\tUTM Easting {}\tUTM Northing\t{}".format(row.POO_UTM_ZONE,
                                                                 row.POO_UTM_EASTING,
                                                                 row.POO_UTM_NORTHING))
    print("ld pm {}\tld twp {}\tld rge {}\tld sec {}\tld qsec {}".format(
                                                        row.POO_LD_PM,
                                                        row.POO_LD_TWP,
                                                        row.POO_LD_RGE,
                                                        row.POO_LD_SEC,
                                                        row.POO_LD_SEC,
                                                        row.POO_LD_QTR_SEC))
    
    
    print("\nSTRUCTURES:\tTOTAL\tRESIDENTIAL\tCOMMERCIAL")
    print("---------------------------------------------------")
    print("Damaged:\t{}\t{}\t\t{}".format(row.STR_DAMAGED,
                                          row.STR_DAMAGED_RES,
                                          row.STR_DAMAGED_COMM))
    print("Destroyed:\t{}\t{}\t\t{}".format(row.STR_DESTROYED,
                                            row.STR_DESTROYED_RES,
                                            row.STR_DESTROYED_COMM))
    print("Threat(max): \t{}\t{}\t\t{}".format(row.STR_THREATENED,
                                               row.STR_THREATENED_RES,
                                               row.STR_THREATENED_COMM))
    
    print("\nCAS/ILL/EVAC \t{}".format(row.EVACUATION_IN_PROGRESS))
    print("Fatalities: {}".format(row.FATALITIES))
    print("Injuries: {}\tThis period: {}".format(row.INJURIES,
                                                 row.INJURIES_TO_DATE))
    #print("Evacuations: {}\tTo date: {}".format(row.NUM_EVACUATED,
    #                                            row.NUM_EVAC_TO_DATE))
    print("Road Closure? {}".format(row.ROAD_CLOSURE_FLAG))

    print("\nFIRE BEHAVIOR: FSR {}".format(row.WF_FSR))
    print("Active? {}\t Backing? {}\t Creeping? {}\t Crowning? {}\tExtreme? {}\tFlanking? {}\tMinimal? {}".format(
                                                row.FB_ACTIVE, row.FB_BACKING, row.FB_CREEPING, row.FB_CROWNING,
                                                row.FB_EXTREME, row.FB_FLANKING, row.FB_MINIMAL))
                                                
    print("Moderate? {}\t Running? {}\t Smoldering?{}\t Spotting?{}\tTorching? {}\tWind Driven? {}".format(
                                                row.FB_MODERATE, row.FB_RUNNING, row.FB_SMOLDERING, row.FB_SPOTTING,
                                                row.FB_TORCHING, row.FB_WIND_DRIVEN))
    '''
    print("Gen: {} Behavior Descriptors: {}, {}, {}".format(row.GEN_FIRE_BEHAVIOR_IDENTIFIER,
                                                    row.FIRE_BEHAVIOR_1_IDENTIFIER,
                                                    row.FIRE_BEHAVIOR_2_IDENTIFIER,
                                                    row.FIRE_BEHAVIOR_3_IDENTIFIER))
    print("Observed Fire Behave: {}\tGrowth Potential {}\tTerrain {}".format(row.OBS_FIRE_BEHAVE,
                                                                 row.GROWTH_POTENTIAL,
                                                                 row.TERRAIN))
    '''
    
    print("\nSUPPRESSION: {} {}".format(row.SUPPRESSION_METHOD,
                                        row.SUPPRESSION_METHOD_FULLNAME))
    
    
    print("\nIMT: GACC Priority {}\tDispatch Priority {}\tIMT Org Desc {}\tJurisdiction {}".format(
                                                                    row.GACC_PRIORITY,
                                                                    row.DISPATCH_PRIORITY,
                                                                    row.IMT_MGMT_ORG_DESC,
                                                                    row.INCIDENT_JURISDICTION))
    print("Demobe Start {}\tStatus {}\tTargets Met? {}\tTotal Aerial {}\tTotal Pers {}".format(
                                              row.PROJ_SIG_RES_DEMOB_START_DATE,
                                              row.STATUS,
                                              row.TARGETS_MET,
                                              row.TOTAL_AERIAL,
                                              row.TOTAL_PERSONNEL))
    print("Unified Cmd? {}".format(row.UNIFIED_COMMAND_FLAG))

    print("\nMISCELLANEOUS")
    print("Area Closure? {}\tCompletion Date {}\tComplex? {}\tEst Damage {}".format(
                                                                    row.AREA_CLOSURE_FLAG,
                                                                    row.ANTICIPATED_COMPLETION_DATE,
                                                                    row.COMPLEX,
                                                                    row.EDAMAGE))
    print("Fuel Model {}\tGrowth Potnl {}\tRisk Assessment {}".format(row.FUEL_MODEL,
                                                                    row.GROWTH_POTENTIAL,
                                                                    row.RISK_ASSESSMENT))
                      
                                                   
            
def print_sitrep_narrative(row):
    """
    print_sitrep_narrative prints all the narrative text fields from the sitrep table for analysis/debugging
    
    Parameters: 
    row - individual row from sitrep table
    
    Returns: none
    """
    
    print_report_block("DAILY NARRATIVE")
    print("Planned Actions: {}\n".format(row.PLANNED_ACTIONS))
    print("Addnl Coop Assist_Org: {}\n".format(row.ADDTNL_COOP_ASSIST_ORG_NARR))
    print("Complexity Level: {}\n".format(row.COMPLEXITY_LEVEL_NARR))
    print("Critical Resource Needs: {}\n".format(row.CRIT_RES_NEEDS_NARR))
    print("Current Threat: {}\n".format(row.CURRENT_THREAT_NARR))
    print("Hazardous Materials: {}\n".format(row.HAZARDS_MATLS_INVOLVMENT_NARR))
    print("Incident Commanders Narrative: {}\n".format(row.INCIDENT_COMMANDERS_NARR))   
    print("Life Safety Health Status: {}\n".format(row.LIFE_SAFETY_HEALTH_STATUS_NARR))
    print("Major Problems: {}\n".format(row.MAJOR_PROBLEMS))
    print("Planned Actions: {}\n".format(row.PLANNED_ACTIONS))  
    print("Potential: {}".format(row.POTENTIAL))
    print("Projected Activity: {}\n".format(row.PROJECTED_ACTIVITY_NARR))
    print("Remarks: {}\n".format(row.REMARKS))
    print("Res Benefits: {}\n".format(row.RES_BENEFITS))
    print("Risk Assessment: {}\n".format(row.RISK_ASSESSMENT))
    print("Significant Events: {}\n".format(row.SIGNIF_EVENTS_SUMMARY))
    print("Strategic Narrative: {}\n".format(row.STRATEGIC_NARR))
    print("Unit or Other: {}\n".format(row.UNIT_OR_OTHER_NARR))
    print("Weather Concerns: {}\n".format(row.WEATHER_CONCERNS_NARR))

def print_full_report(inc_df,sit_df,inc_id):
    """
    print_full_report loops through the incident summary and related sitreps for a single incident_id 
    printing out all related fields including narrative fields
    
    Parameters:
       inc_df: incidents dataframe
       sit_df: sitreps dataframe
       inc_id: incident id for selected incident
       
    Returns: none 
    """
    
    inc_row = inc_df.loc[inc_df.INCIDENT_ID == inc_id]
    print_incident_summary(inc_row)
    df = sit_df.loc[sit_df.INCIDENT_ID == inc_id]
    for i, row in df.iterrows():
        print_sitrep(row)
        print_sitrep_narrative(row)  
        
def plot_trends_and_impacts(sit_df,inc_id,*args,**kwargs):
    """
    plot_trends_and_impacts plots key variables for a given incident over time including:
    * Fire Size (Acres)
    * Daily Growth (FSR)
    * Structures Threatened
    * Structures Destroyed
    * Total Personnel
    * Total Aerial
    * # People Evacuated
    * Estimated Incident Costs (over time)
    
    Parameters:
        sit_df: incident sitreps
        inc_id: incident identifier
        *args
        **kwargs: output = output path for output file 
    """
    
    title_text="Analysis for " + inc_id
    outpath = np.nan
    if 'output' in kwargs:
        outpath = kwargs.get("output")
    
    fig, (ax1,ax2,ax3,ax4,ax5,ax6,ax7,ax8) = plt.subplots(8,1,figsize=(16,30))
    fig.suptitle(title_text, fontsize = 16)
    
    ax1.plot(sit_df.REPORT_TO_DATE,sit_df.ACRES,marker='o')
    ax2.plot(sit_df.REPORT_TO_DATE,sit_df.NEW_ACRES,marker='o')
    ax3.plot(sit_df.REPORT_TO_DATE,sit_df.STR_THREATENED,marker='o')
    ax4.plot(sit_df.REPORT_TO_DATE,sit_df.STR_DESTROYED,marker='o')
    ax5.plot(sit_df.REPORT_TO_DATE,sit_df.TOTAL_PERSONNEL,marker='o')
    ax6.plot(sit_df.REPORT_TO_DATE,sit_df.TOTAL_AERIAL,marker='o')
    ax7.plot(sit_df.REPORT_TO_DATE,sit_df.RPT_EVACUATIONS,marker='o')
    ax8.plot(sit_df.REPORT_TO_DATE,sit_df.PROJECTED_FINAL_IM_COST,marker='o')
    
    # Set titles and axis labels:
    ax1.set(xlabel="Report Date",
            ylabel="Acres",
            title="Fire Size (Acres)")
     # Set titles and axis labels:
    ax2.set(xlabel="Report Date",
            ylabel="Acres",
            title="title=""New Growth (Acres))")
    # Set titles and axis labels:
    ax3.set(xlabel="Report Date",
            ylabel="# Structures",
            title="Structures Threatened")
    # Set titles and axis labels:
    ax4.set(xlabel="Report Date",
            ylabel="# Structures",
            title="Structures Destroyed")
    # Set titles and axis labels:
    ax5.set(xlabel="Report Date",
            ylabel="# Personnel",
            title="Total Personnel (Daily)")
    # Set titles and axis labels:
    ax6.set(xlabel="Report Date",
            ylabel="# Aerial Resources",
            title="Total Aerial (Daily)")
    ax7.set(xlabel="Report Date",
            ylabel="# Evacuated",
            title="# Evacuated (Daily)")
    ax8.set(xlabel="Report Date",
            ylabel="Projected Cost",
            title="Projected Cost")
    
    plt.tight_layout(pad=2)
    if outpath:
        plt.savefig(outpath)
    plt.show()

    

