# Create EJI Tool Version: Beta (5/14/2021)

# This script downloads JSON data from a Census Bureau API for 5-Year ACS Estimates, performs calculations and data formatting in
# Pandas dataframes, and uses ESRI (arcpy) tools to join the results to Block Group and Tract geographies. Outputs are created in
# file geodatabase and CSV formats. It is intended to automate annual production of NCTCOG's Transit Accessibility Improvement Tool (TAIT).
# It is in large part based on a similar tool to streamline production of the annual Environmental Justice Index (EJI).
# Key parameters  are specified by the user in the tool interface, but additional parameters can be customized below.

#Import some libraries
import arcpy
import os
import urllib2
import json
import numpy as np
import pandas as pd


## SOME VARIABLES YOU MIGHT NEED TO CHANGE ##

# List of dictionaries describing the data you're requesting from the Census API at the BLOCK GROUP geography.
# This has to be split into two queries because 50 variables is the upper limit for requests to this API.
# desc_name is a descriptive name of your choosing. This becomes part of the field name. Choose something tidy
# that will result in a recognizable field name later.
# census_name is the variable name as described here: https://api.census.gov/data/2019/acs/acs5/variables.html

bg_desired_col1 =    [{'desc_name': 'Total_Pop', 'census_name': 'B01001_001E'},
                      {'desc_name': 'NotHispLatino_WhiteAlone', 'census_name': 'B03002_003E'},
                      {'desc_name': 'Hispanic', 'census_name': 'B03002_012E'},
                      {'desc_name': 'TotBlk', 'census_name': 'B02001_003E'},
                      {'desc_name': 'TotAI', 'census_name': 'B02001_004E'},
                      {'desc_name': 'TotAsian', 'census_name': 'B02001_005E'},
                      {'desc_name': 'Tot_HPI', 'census_name': 'B02001_006E'},
                      {'desc_name': 'TotOther', 'census_name': 'B02001_007E'},
                      {'desc_name': 'Tot2Race', 'census_name': 'B02001_008E'},
                      {'desc_name': 'TotPSK', 'census_name': 'C17002_001E'},
                      {'desc_name': 'BlwPov_Under50', 'census_name': 'C17002_002E'},
                      {'desc_name': 'BlwPov_50to99', 'census_name': 'C17002_003E'},
                      {'desc_name': 'BlwPov_100to124', 'census_name': 'C17002_004E'},
                      {'desc_name': 'PopOver5', 'census_name': 'B16004_001E'},
                      {'desc_name': 'SpeakSpanish_5_17', 'census_name': 'B16004_004E'},
                      {'desc_name': 'SpeakSpanish_5_17_EnglishVWell', 'census_name': 'B16004_005E'},
                      {'desc_name': 'SpeakIE_5_17', 'census_name': 'B16004_009E'},
                      {'desc_name': 'SpeakIE_5_17_EnglishVWell', 'census_name': 'B16004_010E'},
                      {'desc_name': 'SpeakAsian_5_17', 'census_name': 'B16004_014E'},
                      {'desc_name': 'SpeakAsian_5_17_EnglishVWell', 'census_name': 'B16004_015E'},
                      {'desc_name': 'SpeakOther_5_17', 'census_name': 'B16004_019E'},
                      {'desc_name': 'SpeakOther_5_17_EnglishVWell', 'census_name': 'B16004_020E'},
                      {'desc_name': 'SpeakSpanish_18_64', 'census_name': 'B16004_026E'},
                      {'desc_name': 'SpeakSpanish_18_64_EnglishVWell', 'census_name': 'B16004_027E'},
                      {'desc_name': 'SpeakIE_18_64', 'census_name': 'B16004_031E'},
                      {'desc_name': 'SpeakIE_18_64_EnglishVWell', 'census_name': 'B16004_032E'},
                      {'desc_name': 'SpeakAsian_18_64', 'census_name': 'B16004_036E'},
                      {'desc_name': 'SpeakAsian_18_64_EnglishVWell', 'census_name': 'B16004_037E'},
                      {'desc_name': 'SpeakOther_18_64', 'census_name': 'B16004_041E'},
                      {'desc_name': 'SpeakOther_18_64_EnglishVWell', 'census_name': 'B16004_042E'},
                      {'desc_name': 'SpeakSpanish_65Over', 'census_name': 'B16004_048E'},
                      {'desc_name': 'SpeakSpanish_65Over_EnglishVWell', 'census_name': 'B16004_049E'},
                      {'desc_name': 'SpeakIE_65Over', 'census_name': 'B16004_053E'},
                      {'desc_name': 'SpeakIE_65Over_EnglishVWell', 'census_name': 'B16004_054E'},
                      {'desc_name': 'SpeakAsian_65Over', 'census_name': 'B16004_058E'},
                      {'desc_name': 'SpeakAsian_65Over_EnglishVWell', 'census_name': 'B16004_059E'},
                      {'desc_name': 'SpeakOther_65Over', 'census_name': 'B16004_063E'},
                      {'desc_name': 'SpeakOther_65Over_EnglishVWell', 'census_name': 'B16004_064E'},
                     ]
bg_desired_col2 =    [{'desc_name': 'Age14Under1', 'census_name': 'B01001_003E'},
                      {'desc_name': 'Age14Under2', 'census_name': 'B01001_004E'},
                      {'desc_name': 'Age14Under3', 'census_name': 'B01001_005E'},
                      {'desc_name': 'Age14Under4', 'census_name': 'B01001_027E'},
                      {'desc_name': 'Age14Under5', 'census_name': 'B01001_028E'},
                      {'desc_name': 'Age14Under6', 'census_name': 'B01001_029E'},
                      {'desc_name': 'Pop18Over', 'census_name': 'B21001_001E'},
                      {'desc_name': 'TotalVet', 'census_name': 'B21001_002E'},
                      {'desc_name': 'Age65Over1', 'census_name': 'B01001_020E'},
                      {'desc_name': 'Age65Over2', 'census_name': 'B01001_021E'},
                      {'desc_name': 'Age65Over3', 'census_name': 'B01001_022E'},
                      {'desc_name': 'Age65Over4', 'census_name': 'B01001_023E'},
                      {'desc_name': 'Age65Over5', 'census_name': 'B01001_024E'},
                      {'desc_name': 'Age65Over6', 'census_name': 'B01001_025E'},
                      {'desc_name': 'Age65Over7', 'census_name': 'B01001_044E'},
                      {'desc_name': 'Age65Over8', 'census_name': 'B01001_045E'},
                      {'desc_name': 'Age65Over9', 'census_name': 'B01001_046E'},
                      {'desc_name': 'Age65Over10', 'census_name': 'B01001_047E'},
                      {'desc_name': 'Age65Over11', 'census_name': 'B01001_048E'},
                      {'desc_name': 'Age65Over12', 'census_name': 'B01001_049E'},
                      {'desc_name': 'TotalHH', 'census_name': 'B11005_001E'},
                      {'desc_name': 'FHH_Family', 'census_name': 'B11005_007E'},
                      {'desc_name': 'FHH_NonFamily', 'census_name': 'B11005_010E'},
                      {'desc_name': 'ZCHH_Owner', 'census_name': 'B25044_003E'},
                      {'desc_name': 'ZCHH_Renter', 'census_name': 'B25044_010E'}
                     ]

bg_desired_col3 = [
    {'desc_name': 'means_all', 'census_name': 'B08006_001E'},
    {'desc_name': 'means_drv', 'census_name': 'B08006_002E'},
    {'desc_name': 'means_drvalone', 'census_name': 'B08006_003E'},
    {'desc_name': 'means_drv_hov', 'census_name': 'B08006_004E'},
    {'desc_name': 'means_drv_hov2', 'census_name': 'B08006_005E'},
    {'desc_name': 'means_drv_hov3', 'census_name': 'B08006_006E'},
    {'desc_name': 'means_drv_hov4', 'census_name': 'B08006_007E'} 
]


# List of dictionaries describing the data you're requesting from the Census API at the TRACT geography.
# desc_name is a descriptive name of your choosing. This becomes part of the field name. Choose something tidy
# that will result in a recognizable field name later.
# census_name is the variable name as described here: https://api.census.gov/data/2019/acs/acs5/variables.html

tract_desired_columns = [{'desc_name': 'TotPopTract', 'census_name': 'B18101_001E'},
                         {'desc_name': 'MaleDisabUnder5', 'census_name': 'B18101_004E'},
                         {'desc_name': 'MaleDisab5to17', 'census_name': 'B18101_007E'},
                         {'desc_name': 'MaleDisab18to34', 'census_name': 'B18101_010E'},
                         {'desc_name': 'MaleDisab35to64', 'census_name': 'B18101_013E'},
                         {'desc_name': 'MaleDisab65to74', 'census_name': 'B18101_016E'},
                         {'desc_name': 'MaleDisab75over', 'census_name': 'B18101_019E'},
                         {'desc_name': 'FemDisabUnder5', 'census_name': 'B18101_023E'},
                         {'desc_name': 'FemDisab5to17', 'census_name': 'B18101_026E'},
                         {'desc_name': 'FemDisab18to34', 'census_name': 'B18101_029E'},
                         {'desc_name': 'FemDisab35to64', 'census_name': 'B18101_032E'},
                         {'desc_name': 'FemDisab65to74', 'census_name': 'B18101_035E'},
                         {'desc_name': 'FemDisab75over', 'census_name': 'B18101_038E'}
                        ]
      
## VARIABLES YOU PROBABLY WON'T NEED TO CHANGE ##

# Default output location for geoprocessing tools should be the 'in memory' workspace to keep clutter down and speed up execution.
arcpy.env.workspace = 'in_memory'

# All outputs will be in NAD 83 Texas State Plane North Central.
arcpy.env.outputCoordinateSystem = arcpy.SpatialReference(2276)

# Allows ArcGIS tools to overwrite existing output.
arcpy.env.overwriteOutput = True

# List of county FIPS codes the script will request from the API. Also clips the input
# block group and tract geographies.
counties = ['085','113','121','139','143','221','231','251','257','349','363','367','397','425','439','497']

## VARIABLES POPULATED BY THE TOOL INTERFACE ##

#"GetParameterAsText" is used to pull values that the user specifies before the tool is run.

#ACS Year (Last year of 5-year estimates, i.e. '2019' = 2015-2019 5-Year Estimates)
#Note that this tool will need to be revalidated for year 2020 and beyond as geographies and variables may change.
#year = '2019'
year = arcpy.GetParameterAsText(0)

#Folder into which output will be created. If anything with a matching file name already exists in this folder, it will be deleted.
#output_folder = r'I:\Environmental Coordination\Environmental Justice\EJ for Program Areas\Transit Operations\2021 TAIT - 2019 ACS\API\Test Output Folder'
output_folder = arcpy.GetParameterAsText(1)

#Derives the parent (containing) folder of the output folder.
parent_folder = '\\'.join(output_folder.split('\\')[:-1])

#Lets the user specify the location of layers depicting current block groups and tracts. Can also be feature classes at need.
#GEOID and county fields are exposed here as well because they may change after 2020 updates.
#bg_lyr = r'I:\Environmental Coordination\Environmental Justice\EJ for Program Areas\Transit Operations\2021 TAIT - 2019 ACS\API\Census Block Groups (Current).lyr'
bg_lyr = arcpy.GetParameterAsText(2)

#bg_geoidfield = 'GEOID10'
bg_geoidfield = arcpy.GetParameterAsText(3)

#bg_countyfield = 'COUNTYFP10'
bg_countyfield = arcpy.GetParameterAsText(4)

#tract_lyr = r'I:\Environmental Coordination\Environmental Justice\EJ for Program Areas\Transit Operations\2021 TAIT - 2019 ACS\API\Census Tracts (Current).lyr'
tract_lyr = arcpy.GetParameterAsText(5)

#tract_geoidfield = 'GEOID10'
tract_geoidfield = arcpy.GetParameterAsText(6)

#tract_countyfield = 'COUNTYFP10'
tract_countyfield = arcpy.GetParameterAsText(7)

## INITIALIZE BLOCK GROUP AND TRACT LAYERS - EXECUTION BEGINS HERE ##

#Makes copies of bg and tract geography from input layers including only GEOID field for later joining.

countywhere = "{} IN (".format(bg_countyfield)
for county in counties:
    countywhere = countywhere + "'{}',".format(county)
countywhere = countywhere[:-1] + ")"

for fc in [[bg_lyr,'bg',bg_geoidfield,bg_countyfield],[tract_lyr,'tract',tract_geoidfield,tract_countyfield]]:
    arcpy.AddMessage('Loading {} geography...'.format(fc[1]))

    countywhere = "{} IN (".format(fc[3])
    for county in counties:
        countywhere = countywhere + "'{}',".format(county)
    countywhere = countywhere[:-1] + ")"

    fieldmappings = arcpy.FieldMappings()
    fieldmap = arcpy.FieldMap()
    fieldmap.addInputField(fc[0],fc[2])
    fieldmappings.addFieldMap(fieldmap)
    arcpy.FeatureClassToFeatureClass_conversion(fc[0],arcpy.env.workspace,fc[1],countywhere,fieldmappings)

## TRACT API CALL ##

# Initializes list of column names to be inserted into URL for TRACT API call.
df_init_columns = []
census_column_names = ''

for x in tract_desired_columns:
    df_init_columns.append('{}'.format(x['desc_name']))
    census_column_names = census_column_names + x['census_name'] + ','

df_init_columns.extend(['State','County','Tract'])
census_column_names = census_column_names[:-1]

# Establishes empty pandas dataframe to load data from API into.
arcpy.AddMessage("Creating initial empty dataframe for tracts...")
results_pd_all_tract = pd.DataFrame(columns = df_init_columns)

# Iterates through each county in counties list to connect to the URL, download the results in JSON format, and insert them into the empty data frame.
for county in counties:
    arcpy.AddMessage("Processing county {}...".format(county))

    url = 'https://api.census.gov/data/{}/acs/acs5?get={}&in=state:48%20county:{}&for=tract'.format(year,census_column_names,county)

    arcpy.AddMessage("Requesting URL...")
    apicall = urllib2.urlopen(url)

    arcpy.AddMessage("Loading results text...")
    results = apicall.read()

    arcpy.AddMessage("Parsing into json...")
    results_json = json.loads(results)[1:]

    arcpy.AddMessage("Loading JSON into Pandas dataframe...")
    results_pd = pd.DataFrame(columns = df_init_columns, data = results_json)

    results_pd_all_tract = results_pd_all_tract.append(results_pd)

#Establish a new column in the dataframe with a concatenated GEOID from State, County, and Tract FIPS codes.
arcpy.AddMessage("Calculating tract geoid for all counties...")
results_pd_all_tract['Tract_GEOID'] = results_pd_all_tract['State'] + results_pd_all_tract['County'] + results_pd_all_tract['Tract']

## TRACT CALCULATIONS AND REFORMATTING ##

arcpy.AddMessage('Usings Pandas to calculate and reorder fields (Tracts)...')

for col in list(results_pd_all_tract.columns):
    if col not in ['Tract_GEOID','County']:
        results_pd_all_tract[col] = results_pd_all_tract[col].astype('int64')

results_pd_all_tract['WholeTract_PWD'] = 0
for gender in ['Male','Fem']:
    for age in ['Under5','5to17','18to34','35to64','65to74','75over']:
        results_pd_all_tract['WholeTract_PWD'] = results_pd_all_tract['WholeTract_PWD'] + results_pd_all_tract['{}Disab{}'.format(gender,age)]


## BLOCK GROUP API CALL 1 ##
   
# Initializes list of column names to be inserted into URL for BLOCK GROUP API call.
df_init_columns = []
census_column_names = ''

for x in bg_desired_col1:
    df_init_columns.append('{}'.format(x['desc_name']))
    census_column_names = census_column_names + x['census_name'] + ','

df_init_columns.extend(['State','County','Tract','BG'])
census_column_names = census_column_names[:-1]

# Establishes empty pandas dataframe to load data from BLOCK GROUP API call into.
arcpy.AddMessage("Creating initial empty dataframe for block groups...")
results_pd_all_bg1 = pd.DataFrame(columns = df_init_columns)

# Iterates through each county in counties list to connect to the URL, download the results in JSON format, and insert them into the empty data frame.
for county in counties:
    arcpy.AddMessage("Processing county {}...".format(county))

    url = 'https://api.census.gov/data/{}/acs/acs5?get={}&in=state:48%20county:{}&for=block%20group'.format(year,census_column_names,county)

    arcpy.AddMessage("Requesting URL...")
    
    apicall = urllib2.urlopen(url)

    arcpy.AddMessage("Loading results text...")
    results = apicall.read()

    arcpy.AddMessage("Parsing into json...")
    results_json = json.loads(results)[1:]

    arcpy.AddMessage("Loading JSON into Pandas dataframe...")
    results_pd = pd.DataFrame(columns = df_init_columns, data = results_json)

    results_pd_all_bg1 = results_pd_all_bg1.append(results_pd)

#Establish a new column in the dataframe with a concatenated GEOID from State, County, Tract, and BG FIPS codes.
arcpy.AddMessage("Calculating tract geoid for all counties...")
results_pd_all_bg1['GEOID'] = results_pd_all_bg1['State'] + results_pd_all_bg1['County'] + results_pd_all_bg1['Tract'] + results_pd_all_bg1['BG']

## BLOCK GROUP API CALL 2 ##
   
# Initializes list of column names to be inserted into URL for BLOCK GROUP API call.
df_init_columns = []
census_column_names = ''

for x in bg_desired_col2:
    df_init_columns.append('{}'.format(x['desc_name']))
    census_column_names = census_column_names + x['census_name'] + ','

df_init_columns.extend(['State','County','Tract','BG'])
census_column_names = census_column_names[:-1]

# Establishes empty pandas dataframe to load data from BLOCK GROUP API call into.
arcpy.AddMessage("Creating initial empty dataframe for block groups...")
results_pd_all_bg2 = pd.DataFrame(columns = df_init_columns)

# Iterates through each county in counties list to connect to the URL, download the results in JSON format, and insert them into the empty data frame.
for county in counties:
    arcpy.AddMessage("Processing county {}...".format(county))

    url = 'https://api.census.gov/data/{}/acs/acs5?get={}&in=state:48%20county:{}&for=block%20group'.format(year,census_column_names,county)

    arcpy.AddMessage("Requesting URL...")
    apicall = urllib2.urlopen(url)

    arcpy.AddMessage("Loading results text...")
    results = apicall.read()

    arcpy.AddMessage("Parsing into json...")
    results_json = json.loads(results)[1:]

    arcpy.AddMessage("Loading JSON into Pandas dataframe...")
    results_pd = pd.DataFrame(columns = df_init_columns, data = results_json)

    results_pd_all_bg2 = results_pd_all_bg2.append(results_pd)

#Establish a new column in the dataframe with a concatenated GEOID from State, County, Tract, and BG FIPS codes.
arcpy.AddMessage("Calculating tract geoid for all counties...")
results_pd_all_bg2['GEOID'] = results_pd_all_bg2['State'] + results_pd_all_bg2['County'] + results_pd_all_bg2['Tract'] + results_pd_all_bg2['BG']
results_pd_all_bg2['Tract_GEOID'] = results_pd_all_bg2['State'] + results_pd_all_bg2['County'] + results_pd_all_bg2['Tract']


#Join the two BG-level data frames together.
results_pd_notract_bg = pd.merge(results_pd_all_bg1,results_pd_all_bg2,on='GEOID')

#Join tract-level PWD data to block groups.
results_pd_all_bg = pd.merge(results_pd_notract_bg,results_pd_all_tract,on='Tract_GEOID',how='left',suffixes=('_x','_y'))

## BLOCK GROUP CALCULATIONS AND REFORMATTING ##

arcpy.AddMessage('Usings Pandas to calculate and reorder fields (Block Groups)...')

for col in list(results_pd_all_bg.columns):
    if col not in ['GEOID','County_x']:
        results_pd_all_bg[col] = results_pd_all_bg[col].astype('int64')

results_pd_all_bg['TotalMin'] = results_pd_all_bg['Total_Pop'] - results_pd_all_bg ['NotHispLatino_WhiteAlone']

results_pd_all_bg['BlwPov'] = results_pd_all_bg['BlwPov_Under50'] + results_pd_all_bg['BlwPov_50to99'] + results_pd_all_bg['BlwPov_100to124']

results_pd_all_bg['TotalLEP'] = 0
for language in ['Spanish','IE','Asian','Other']:
    results_pd_all_bg['{}LEP'.format(language)] = 0
    for age in ['5_17','18_64','65Over']:
        results_pd_all_bg['{}LEP'.format(language)] += (results_pd_all_bg['Speak{}_{}'.format(language,age)] - results_pd_all_bg['Speak{}_{}_EnglishVWell'.format(language,age)])
    results_pd_all_bg['TotalLEP'] += results_pd_all_bg['{}LEP'.format(language)]

results_pd_all_bg['Age65Over'] = 0
for i in range(1,12+1):
    results_pd_all_bg['Age65Over'] += results_pd_all_bg['Age65Over{}'.format(str(i))]

results_pd_all_bg['Age14Under'] = 0
for i in range(1,6+1):
    results_pd_all_bg['Age14Under'] += results_pd_all_bg['Age14Under{}'.format(str(i))]

results_pd_all_bg['TotalFHH'] = results_pd_all_bg['FHH_Family'] + results_pd_all_bg['FHH_NonFamily']

results_pd_all_bg['NoCar'] = results_pd_all_bg['ZCHH_Owner'] + results_pd_all_bg['ZCHH_Renter']

results_pd_all_bg['Sum_PWD'] = results_pd_all_bg['WholeTract_PWD'].astype('float64') * (results_pd_all_bg['Total_Pop'].astype('float64') / results_pd_all_bg['TotPopTract'].astype('float64'))


calculation_fields = [{'variable': 'TotalMin', 'universe': 'Total_Pop', 'pct': 'Pct_TotMin', 'ratio': 'Rat_TotMin'},
                      {'variable': 'Hispanic', 'universe': 'Total_Pop', 'pct': 'Pct_Hisp', 'ratio': 'Rat_Hisp'},
                      {'variable': 'TotBlk', 'universe': 'Total_Pop', 'pct': 'Pct_TotBlk', 'ratio': 'Rat_TotBlk'},
                      {'variable': 'TotAI', 'universe': 'Total_Pop', 'pct': 'Pct_TotAI', 'ratio': 'Rat_TotAI'},
                      {'variable': 'TotAsian', 'universe': 'Total_Pop', 'pct': 'Pct_TotAsn', 'ratio': 'Rat_TotAsn'},
                      {'variable': 'Tot_HPI', 'universe': 'Total_Pop', 'pct': 'Pct_TotHPI', 'ratio': 'Rat_TotHPI'},
                      {'variable': 'TotOther', 'universe': 'Total_Pop', 'pct': 'Pct_TotOth', 'ratio': 'Rat_TotOth'},
                      {'variable': 'Tot2Race', 'universe': 'Total_Pop', 'pct': 'Pct_Tot2Ra', 'ratio': 'Rat_Tot2Ra'},
                      {'variable': 'BlwPov', 'universe': 'TotPSK', 'pct': 'Pct_BlwPov', 'ratio': 'Rat_BlwPov'},
                      {'variable': 'TotalLEP', 'universe': 'PopOver5', 'pct': 'Pct_TotLEP', 'ratio': 'Rat_TotLEP'},
                      {'variable': 'SpanishLEP', 'universe': 'PopOver5', 'pct': 'Pct_SpLEP', 'ratio': 'Rat_SpLEP'},
                      {'variable': 'IELEP', 'universe': 'PopOver5', 'pct': 'Pct_IE_LEP', 'ratio': 'Rat_IE_LEP'},
                      {'variable': 'AsianLEP', 'universe': 'PopOver5', 'pct': 'Pct_AsnLEP', 'ratio': 'Rat_AsnLEP'},
                      {'variable': 'OtherLEP', 'universe': 'PopOver5', 'pct': 'Pct_OthLEP', 'ratio': 'Rat_OthLEP'},
                      {'variable': 'Age65Over', 'universe': 'Total_Pop', 'pct': 'Pct65_Over', 'ratio': 'Rat_65Over'},
                      {'variable': 'TotalFHH', 'universe': 'TotalHH', 'pct': 'Pct_TotFHH', 'ratio': 'Rat_TotFHH'},
                      {'variable': 'NoCar', 'universe': 'TotalHH', 'pct': 'Pct_NoCar', 'ratio': 'Rat_NoCar'},
                      {'variable': 'Age14Under', 'universe': 'Total_Pop', 'pct': 'Pct14_Unde', 'ratio': 'Rat_14Unde'},
                      {'variable': 'TotalVet', 'universe': 'Pop18Over', 'pct': 'Pct_Vet', 'ratio': 'Rat_Vet'},
                      {'variable': 'Sum_PWD', 'universe': 'Total_Pop', 'pct': 'Pct_PWD', 'ratio':'Rat_PWD'}
                     ]                      

for field in calculation_fields:
    results_pd_all_bg[field['pct']] = (results_pd_all_bg[field['variable']] / results_pd_all_bg[field['universe']])
#    regional_pct = ((np.asarray(results_pd_all_bg.iloc[:,results_pd_all_bg.columns.get_loc(field['variable'])], dtype=np.float64).sum()) / (np.asarray(results_pd_all_bg.iloc[:,results_pd_all_bg.columns.get_loc(field['universe'])], dtype=np.float64).sum()))
    regional_pct = float(results_pd_all_bg[field['variable']].sum()) / float(results_pd_all_bg[field['universe']].sum())
    results_pd_all_bg[field['ratio']] = results_pd_all_bg[field['pct']] / regional_pct

results_pd_all_bg.loc[results_pd_all_bg['Rat_65Over'] >= 1.0, 'ARP_65Over'] = 'Y'
results_pd_all_bg.loc[results_pd_all_bg['Rat_65Over'] < 1.0, 'ARP_65Over'] = 'N'

results_pd_all_bg.loc[results_pd_all_bg['Rat_BlwPov'] >= 1.0, 'ARP_BlwPov'] = 'Y'
results_pd_all_bg.loc[results_pd_all_bg['Rat_BlwPov'] < 1.0, 'ARP_BlwPov'] = 'N'

results_pd_all_bg.loc[results_pd_all_bg['Rat_PWD'] >= 1.0, 'ARP_PWD'] = 'Y'
results_pd_all_bg.loc[results_pd_all_bg['Rat_PWD'] < 1.0, 'ARP_PWD'] = 'N'

results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '085', 'CountyText'] = 'Collin'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '113', 'CountyText'] = 'Dallas'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '121', 'CountyText'] = 'Denton'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '139', 'CountyText'] = 'Ellis'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '221', 'CountyText'] = 'Hood'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '231', 'CountyText'] = 'Hunt'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '251', 'CountyText'] = 'Johnson'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '257', 'CountyText'] = 'Kaufman'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '367', 'CountyText'] = 'Parker'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '397', 'CountyText'] = 'Rockwall'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '439', 'CountyText'] = 'Tarrant'
results_pd_all_bg.loc[results_pd_all_bg['County_x'] == '497', 'CountyText'] = 'Wise'

#results_bg_reordered = results_pd_all_bg[['GEOID','CountyText','Total_Pop','TotalMin','Pct_TotMin','Rat_TotMin','Hispanic','Pct_Hisp','Rat_Hisp','TotBlk','Pct_TotBlk','Rat_TotBlk','TotAI','Pct_TotAI','Rat_TotAI','TotAsian','Pct_TotAsn','Rat_TotAsn','Tot_HPI','Pct_TotHPI','Rat_TotHPI','TotOther','Pct_TotOth','Rat_TotOth','Tot2Race','Pct_Tot2Ra','Rat_Tot2Ra','TotPSK','BlwPov','Pct_BlwPov','Rat_BlwPov','PopOver5','TotalLEP','Pct_TotLEP','Rat_TotLEP','SpanishLEP','Pct_SpLEP','Rat_SpLEP','IELEP','Pct_IE_LEP','Rat_IE_LEP','AsianLEP','Pct_AsnLEP','Rat_AsnLEP','OtherLEP','Pct_OthLEP','Rat_OthLEP','Age65Over','Pct65_Over','Rat_65Over','TotalHH','TotalFHH','Pct_TotFHH','Rat_TotFHH','NoCar','Pct_NoCar','Rat_NoCar','Min_RegPct','Pov_RegPct','Both_RegPct']]
results_bg_reordered = results_pd_all_bg[['GEOID','Tract_GEOID','CountyText','Total_Pop','TotalMin','Pct_TotMin','Hispanic','Pct_Hisp','TotBlk','Pct_TotBlk','TotAI','Pct_TotAI','TotAsian','Pct_TotAsn','Tot_HPI','Pct_TotHPI','TotOther','Pct_TotOth','Tot2Race','Pct_Tot2Ra','TotPSK','BlwPov','Pct_BlwPov','Rat_BlwPov','ARP_BlwPov','PopOver5','TotalLEP','Pct_TotLEP','SpanishLEP','Pct_SpLEP','IELEP','Pct_IE_LEP','AsianLEP','Pct_AsnLEP','OtherLEP','Pct_OthLEP','Age65Over','Pct65_Over','Rat_65Over','ARP_65Over','TotalHH','NoCar','Pct_NoCar','Rat_NoCar','Age14Under','Pct14_Unde','Rat_14Unde','Pop18Over','TotalVet','Pct_Vet','Rat_Vet','TotPopTract','Sum_PWD','Pct_PWD','Rat_PWD','ARP_PWD']]

results_bg_reordered = results_bg_reordered.rename(columns={'CountyText':'County'})
results_bg_reordered = results_bg_reordered.rename(columns={'Tract_GEOID':'TractID'})

## BG OUTPUTS ##

if not arcpy.Exists(output_folder):
    if arcpy.Exists(parent_folder):
        arcpy.AddMessage('Specified output folder does not exist. Creating it now to store output...')
        os.mkdir(output_folder)

arcpy.AddMessage('Writing output csv for BG...')

if arcpy.Exists(r'{}\EJI_{}_BG.csv'.format(output_folder,year)):
    arcpy.Delete_management(r'{}\TAIT_{}ACS.csv'.format(output_folder,year))

results_bg_reordered.to_csv(r'{}\TAIT_{}ACS.csv'.format(output_folder,year),index = None, header = True)

arcpy.AddMessage('Importing to ArcGIS table and joining to BG geography...')
arcpy.TableToTable_conversion(r'{}\TAIT_{}ACS.csv'.format(output_folder,year),arcpy.env.workspace,'eji_bg')

arcpy.JoinField_management('bg',bg_geoidfield,'eji_bg','GEOID')

arcpy.DeleteField_management('bg','GEOID')

arcpy.AddMessage('Calculating land area...')
arcpy.AddField_management('bg','LandSqM','DOUBLE')
arcpy.CalculateField_management('bg','LandSqM','!Shape.Area@SQUAREMILES!','PYTHON')

arcpy.AddMessage('Calculating pop density...')
arcpy.AddField_management('bg','PopDen','DOUBLE')
arcpy.CalculateField_management('bg','PopDen','!Total_Pop!/!Shape.Area@SQUAREMILES!','PYTHON')

arcpy.AddMessage('Copying output feature class...')

if arcpy.Exists(r'{}\TAIT_{}ACS.gdb'.format(output_folder,year)):
    arcpy.Delete_management(r'{}\TAIT_{}ACS.gdb'.format(output_folder,year))    

arcpy.CreateFileGDB_management(output_folder,'TAIT_{}ACS.gdb'.format(year))

arcpy.CopyFeatures_management('bg',r'{}\TAIT_{}ACS.gdb\TAIT_{}ACS'.format(output_folder,year,year))




