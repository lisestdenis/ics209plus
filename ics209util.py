import pandas as pd
import numpy as np
from datetime import timedelta
import re

def is_sitrep(year, file_suffix):
    """Determines if current file is setrep file based on suffix
    
    Args:
        year: current year determines file naming convention
        file_suffix: current file suffix

    Returns: true/false
    """
    if year < 2014:
        if file_suffix in ['S','S_T','INFORMATIONS'] :
            return True
    else: # 2014+
        if file_suffix == '_209_REPORTS' :
            return True
    return False


def clean_narrative_text(narrText):
    """
    Strip values equivalent to empty string (e.g. None, Same, null...)
    args:
        narrText - string to be cleaned
    returns:
        version of the string without null values or np.nan for empty strings
    """
    narrText = str(narrText)
    narrText = narrText.strip().lower()
    narrText = narrText.replace('null','') # replace the word 'null'
    narrText = narrText.replace("n/a","") # replace the value "n/a"
    narrText = re.sub('(there are)*\s* none[ a-z\.]*','',narrText) # delete variations on "none..."
    narrText = re.sub('(there are)*\s*no [a-z \.,\-]*','',narrText) # delete varations on "no ..."
    narrText = re.sub('(same[a-z \.,\-]*)','',narrText) # delete varations on "no ..."
    narrText = narrText.strip()
    if narrText:
        return narrText
    else:
        return np.nan   

def remove_problematic_chars(year, df, legacyFlag):
    """
    Remove hidden characters from free-form text fields in the sitrep records. 
    The files originate from IBM Cognos and the free-form text fields can contain the '\r' (carriage return) character. 
    Newline characters are also removed as part of the cleaning.

    Parameters
    ----------
    year : the current year. 2001-2013 are part of historical tables, 2014+ are part of new system. The fields
            on the 209 form change from year to year, so year is used to identify which fields need cleaning.
    df: the current data frame being processed
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
        df.loc[df['DISCOVERY_DATE'].notnull(),'DISCOVERY_DATE'] = epochStart + pd.to_timedelta(np.floor(df.DISCOVERY_DATE * 86400),'s')
        df.loc[df['APPROVED_DATE'].notnull(),'APPROVED_DATE'] = epochStart + pd.to_timedelta(np.floor(df.APPROVED_DATE * 86400),'s')
        df.loc[df['CREATED_DATE'].notnull(),'CREATED_DATE'] = epochStart + pd.to_timedelta(np.floor(df.CREATED_DATE * 86400),'s')
        df.loc[df['LAST_MODIFIED_DATE'].notnull(),'LAST_MODIFIED_DATE'] = epochStart + pd.to_timedelta(np.floor(df.LAST_MODIFIED_DATE * 86400),'s')
        df.loc[df['PREPARED_DATE'].notnull(),'PREPARED_DATE'] = epochStart + pd.to_timedelta(np.floor(df.PREPARED_DATE * 86400),'s')
        # The only date not in MSTimestamp format. Not sure why!
        df['PROJ_SIG_RES_DEMOB_START_DATE'] = pd.to_datetime(df.PROJ_SIG_RES_DEMOB_START_DATE)
        df.loc[df['REPORT_TO_DATE'].notnull(),'REPORT_TO_DATE'] = epochStart + pd.to_timedelta(np.floor(df.REPORT_TO_DATE * 86400),'s')
        df.loc[df['REPORT_FROM_DATE'].notnull(),'REPORT_FROM_DATE'] = epochStart + pd.to_timedelta(np.floor(df.REPORT_FROM_DATE * 86400),'s')
        df.loc[df['SUBMITTED_DATE'].notnull(),'SUBMITTED_DATE'] = epochStart + pd.to_timedelta(np.floor(df.SUBMITTED_DATE * 86400),'s')
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
    
    lrg_ptr = fod_list[0]
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

