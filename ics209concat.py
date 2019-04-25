import pandas as pd
import numpy as np
import ics209util

lgcy_timespan = '1999to2002'
hist_timespan = '2001to2013'
curr_timespan = '2014' # truncated until FAMWEB issues are fixed
current_year = 2015

lgcy_sitrep_data = []
lgcy_resource_data = []
lgcy_structure_data = []
lgcy_lookup_data = []
hist_sitrep_data = []
hist_resource_data = []
hist_structure_data = []
hist_complex_data = []
hist_lookup_data = []
curr_incident_data = []
curr_sitrep_data = []
curr_resource_data = []
curr_structure_data = []
curr_cslty_data = []
curr_life_safety_data = []
curr_strategy_data = []
curr_lookup_data = []
    
def get_annual_famweb_datafile(year, file_suffix):
    """Retrieve file from annual directory and convert to dataframe
    
    Args:
        year: year of file
        file_suffix: suffix for file being retrieved
    
    Returns: pandas dataframe
    """
    legacy_suffixes = ['INFORMATIONS','RESOURCES','STRUCTURES']
    # Setup the appropriate file prefix based on current system or historical
    if year < 2001 :
        xlPath = "../../data/raw/excel/{}/IMSR_INCIDENT_{}.xlsx".format(year,file_suffix)
    elif year < 2014 :
        if ((year == 2001) or (year == 2002)) and (file_suffix in legacy_suffixes) :
            xlPath = "../../data/raw/excel/{}/IMSR_INCIDENT_{}.xlsx".format(year,file_suffix)
        else:
            xlPath = "../../data/raw/excel/{}/IMSR_IMSR_209_INCIDENT{}.xlsx".format(year,file_suffix)
    else :
        xlPath = "../../data/raw/excel/{}/SIT209_HISTORY_INCIDENT{}.xlsx".format(year,file_suffix)
    
    # get the excel file and convert to df
    xl = pd.ExcelFile(xlPath)
    df = xl.parse(0)
    # if this is setrep, remove \n & \r chars to prevent problems
    if file_suffix == 'INFORMATIONS' :
        legacyFlag = True
    else:
        legacyFlag = False
    if ics209util.is_sitrep(year,file_suffix):
        df = ics209util.remove_problematic_chars(year,df,legacyFlag)
        if year == 2015:
            df = ics209util.reformatMSTimestamp(df,file_suffix)
        elif year == 2016:
            df = ics209util.reformatTimestamp(df,file_suffix)
    return(df)

def get_annual_lookup_file(year):
    """ Get annual lookup file and return convert to dataframe
    
    Args: 
        year: current year
    Returns: 
        dataframe
    """
    if year < 2014:
        xlPath = "../../data/raw/excel/{}/IMSR_LOOKUPS.xlsx".format(year)
    else :
        if year == 2016 :
            xlPath = "../../data/raw/excel/{}/SIT209_LOOKUP_CODES.xlsx".format(year)
        else:
            xlPath = "../../data/raw/excel/{}/SIT209_HISTORY_SIT209_LOOKUP_CODES.xlsx".format(year)
    xl = pd.ExcelFile(xlPath)
    return(xl.parse(0))

def concatenate_annual_files():
    """ concatenate_annual_files: processes each year of ics209 data and consolidates excel files into single dataframes
    
    Args: none
    """

    # loop from start of hist1 through current year
    for year in range(1999,current_year): #Will retrieve up to current year -1
        print("Getting data for {}...".format(year))
        if year < 2001 :
            lgcy_sitrep_data.append(get_annual_famweb_datafile(year,'INFORMATIONS'))
            lgcy_resource_data.append(get_annual_famweb_datafile(year,'RESOURCES'))
            lgcy_structure_data.append(get_annual_famweb_datafile(year,'STRUCTURES'))
            hist_lookup_data.append(get_annual_lookup_file(year))
        elif year < 2014 : # historical data
            if year == 2001 or year == 2002 :
                lgcy_sitrep_data.append(get_annual_famweb_datafile(year,'INFORMATIONS'))
                lgcy_resource_data.append(get_annual_famweb_datafile(year,'RESOURCES'))
                lgcy_resource_data.append(get_annual_famweb_datafile(year,'STRUCTURES'))
            if year == 2013:
                hist_sitrep_data.append(get_annual_famweb_datafile(year,'S_T')) # file name different in 2013
            else :
                hist_sitrep_data.append(get_annual_famweb_datafile(year,'S')) # append S to INCIDENT
            if year > 2009: # get complex data
                hist_complex_data.append(get_annual_famweb_datafile(year,'_COMPLEX'))
            hist_resource_data.append(get_annual_famweb_datafile(year,'_RESOURCES'))
            hist_structure_data.append(get_annual_famweb_datafile(year,'_STRUCTURES'))
            hist_lookup_data.append(get_annual_lookup_file(year))
        else:
            curr_incident_data.append(get_annual_famweb_datafile(year,'S'))
            curr_sitrep_data.append(get_annual_famweb_datafile(year,'_209_REPORTS'))
            curr_resource_data.append(get_annual_famweb_datafile(year,'_209_RES_UTILIZATIONS'))
            curr_structure_data.append(get_annual_famweb_datafile(year,'_209_AFFECTED_STRUCTS'))
            curr_cslty_data.append(get_annual_famweb_datafile(year,'_209_CSLTY_ILLNESSES'))
            curr_life_safety_data.append(get_annual_famweb_datafile(year,'_209_LIFE_SAFETY_MGMTS'))
            curr_strategy_data.append(get_annual_famweb_datafile(year,'_209_STRATEGIES'))
            curr_lookup_data.append(get_annual_lookup_file(year))
            
    # concatenate individual files and save to csv
    pd.concat(lgcy_sitrep_data).to_csv("../../data/out/IMSR_INCIDENT_INFORMATIONS_{}.csv".format(lgcy_timespan))
    pd.concat(lgcy_resource_data).to_csv("../../data/out/IMSR_INCIDENT_RESOURCES_{}.csv".format(lgcy_timespan))
    pd.concat(lgcy_structure_data).to_csv("../../data/out/IMSR_INCIDENT_STRUCTURES_{}.csv".format(lgcy_timespan))
    pd.concat(hist_sitrep_data).to_csv('../../data/out/IMSR_IMSR_209_INCIDENTS_{}.csv'.format(hist_timespan))
    pd.concat(hist_resource_data).to_csv('../../data/out/IMSR_IMSR_209_INCIDENT_RESOURCES_{}.csv'.format(hist_timespan))
    pd.concat(hist_structure_data).to_csv('../../data/out/IMSR_IMSR_209_INCIDENT_STRUCTURES_{}.csv'.format(hist_timespan))
    pd.concat(hist_complex_data).to_csv('../../data/out/IMSR_IMSR_209_COMPLEX_{}.csv'.format(hist_timespan))
    pd.concat(hist_lookup_data).to_csv('../../data/out/IMSR_LOOKUPS.csv')
    pd.concat(curr_incident_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENTS_{}.csv'.format(curr_timespan))
    curr_sitrep_df = pd.concat(curr_sitrep_data)
    curr_sitrep_df = curr_sitrep_df.drop(['ADDTNL_COOP_ASSIST_ORG_NARR','ELEC_GEOSP_DATA_INCL',\
                                          'DAMAGE_ASSESSMENT_INFO'],axis=1)
    curr_sitrep_df.to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_REPORTS_{}.csv'.format(curr_timespan),\
                          date_format="%Y-%m-%d %H:%M:%S")
    #pd.concat(curr_sitrep_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_REPORTS_{}.csv'.format(curr_timespan))
    pd.concat(curr_resource_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_RES_UTILIZATIONS_{}.csv'\
                                         .format( curr_timespan))
    pd.concat(curr_structure_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_AFFECTED_STRUCTS_{}.csv'\
                                          .format(curr_timespan))
    pd.concat(curr_cslty_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_CSLTY_ILLNESSES_{}.csv'.format(curr_timespan))
    pd.concat(curr_life_safety_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_LIFE_SAFETY_MGMTS_{}.csv'\
                                            .format(curr_timespan))
    pd.concat(curr_strategy_data).to_csv('../../data/out/SIT209_HISTORY_INCIDENT_209_STRATEGIES_{}.csv'.format(curr_timespan))
    pd.concat(curr_lookup_data).to_csv('../../data/out/SIT209_LOOKUP_CODES.csv')
    # Common data for the current system
    common_data = ["COMMONDATA_NWCG_AGENCIES","COMMONDATA_NWCG_UNITS","COMMONDATA_STATES"]
    for d in common_data:
        xlPath = "../../data/raw/excel/2014/{}.xlsx".format(d)
        xl = pd.ExcelFile(xlPath, parse_dates=True)
        df = xl.parse(0)
        df.to_csv("../../data/out/{}_2014.csv".format(d))
    
                                                                                   

