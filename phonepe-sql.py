from git import Repo
import os
import pandas as pd
import numpy as np
import mysql.connector

repo_url= "https://github.com/PhonePe/pulse.git"
clone_path="/home/arunv/Desktop/phonepe/gitrepo"
if not os.path.exists(clone_path):
    os.makedirs(clone_path)
myrepo_path=os.path.join(clone_path,os.path.basename(repo_url).removesuffix('.git').title())
#print(repo_path)
Repo.clone_from(repo_url,myrepo_path)
directory = os.path.join(myrepo_path,'data')
#print(directory)
#########################################################

def rename(directory):
    for root, dirs, files in os.walk(directory):
        if 'state' in dirs:
            state_dir = os.path.join(root, 'state')
        #print(root)
            for state_folder in os.listdir(state_dir):
                # rename the state folder
                old_path = os.path.join(state_dir, state_folder)
                new_path = os.path.join(state_dir, state_folder.title().replace('-', '_').replace('&', 'and'))
                os.rename(old_path, new_path)
    print("Renamed all sub-directories successfully")
                
# Function to extract all paths that has sub-directory in the name of 'state'

def extract_paths(directory):
    path_list = []
    for root, dirs, files in os.walk(directory):
        if os.path.basename(root) == 'state':
            path_list.append(root.replace('\\', '/'))
    return path_list
rename(directory)
#print(extract_paths(directory))
###############################################################
state_directories = extract_paths(directory)
#print(state_directories)
##Agg transaction data statewise
state_path = state_directories[0]
state_list = os.listdir(state_path)
agg_trans_dict = {
                  'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [],
                  'Transaction_count': [], 'Transaction_amount': []
                  }


for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
 
            for transaction_data in df['data']['transactionData']:
                    
                my_type = transaction_data['name']
                count = transaction_data['paymentInstruments'][0]['count']
                amount = transaction_data['paymentInstruments'][0]['amount']
                    
                    # Appending to agg_trans_dict
                    
                agg_trans_dict['State'].append(state)
                agg_trans_dict['Year'].append(year)
                agg_trans_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                agg_trans_dict['Transaction_type'].append(my_type)
                agg_trans_dict['Transaction_count'].append(count)
                agg_trans_dict['Transaction_amount'].append(amount)
            
            
            
agg_trans_df = pd.DataFrame(agg_trans_dict)
########################################################################
state_path = state_directories[1]
state_list = os.listdir(state_path)
agg_user_dict = {
                 'State': [], 'Year': [], 'Quarter': [], 'Brand': [],
                 'Transaction_count': [], 'Percentage': []
                 }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for user_data in df['data']['usersByDevice']:

                    brand = user_data['brand']
                    count = user_data['count']
                    percent = user_data['percentage']
                    
                    # Appending to agg_user_dict
                    
                    agg_user_dict['State'].append(state)
                    agg_user_dict['Year'].append(year)
                    agg_user_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    agg_user_dict['Brand'].append(brand)
                    agg_user_dict['Transaction_count'].append(count)
                    agg_user_dict['Percentage'].append(percent)
            except:
                pass

agg_user_df = pd.DataFrame(agg_user_dict)
##############################################################################
state_path = state_directories[2]
state_list = os.listdir(state_path)
map_trans_dict = {
                    'State': [], 'Year': [], 'Quarter': [],
                    'Transaction_count': [], 'Transaction_amount': []
                    }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for transaction_data in df['data']['hoverDataList']:
                   
                    district = transaction_data['name']
                    count = transaction_data['metric'][0]['count']
                    amount = transaction_data['metric'][0]['amount']
                    
                    # Appending to map_trans_dict
                    
                    map_trans_dict['State'].append(state)
                    map_trans_dict['Year'].append(year)
                    map_trans_dict['Quarter'].append(int(quarter.removesuffix('.json'))) 
                    #map_trans_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_trans_dict['Transaction_count'].append(count)
                    map_trans_dict['Transaction_amount'].append(amount)
            except:
                pass

map_trans_df = pd.DataFrame(map_trans_dict)
################################################################
state_path = state_directories[3]
state_list = os.listdir(state_path)
map_user_dict = {
                 'State': [], 'Year': [], 'Quarter': [],
                 'Registered_users': [], 'App_opens': []
                 }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for district, user_data in df['data']['hoverData'].items():
                    
                    reg_user_count = user_data['registeredUsers']
                    app_open_count = user_data['appOpens']
                    
                    # Appending to map_user_dict
                    
                    map_user_dict['State'].append(state)
                    map_user_dict['Year'].append(year)
                    map_user_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    #map_user_dict['District'].append(district.removesuffix(' district').title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    map_user_dict['Registered_users'].append(reg_user_count)
                    map_user_dict['App_opens'].append(app_open_count)
            except:
                pass

map_user_df = pd.DataFrame(map_user_dict)
##########################################################################
state_path = state_directories[4]
state_list = os.listdir(state_path)
top_trans_dist_dict = {
                        'State': [], 'Year': [], 'Quarter': [],
                        'Transaction_count': [], 'Transaction_amount': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for district_data in df['data']['districts']:
                    
                    name = district_data['entityName']
                    count = district_data['metric']['count']
                    amount = district_data['metric']['amount']
                    
                    # Appending to top_trans_dist_dict
                    
                    top_trans_dist_dict['State'].append(state)
                    top_trans_dist_dict['Year'].append(year)
                    top_trans_dist_dict['Quarter'].append(int(quarter.removesuffix('.json')))                    
                    #top_trans_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_trans_dist_dict['Transaction_count'].append(count)
                    top_trans_dist_dict['Transaction_amount'].append(amount)
            except:
                pass

top_trans_dist_df = pd.DataFrame(top_trans_dist_dict)
#########################################################################
state_path = state_directories[4]
state_list = os.listdir(state_path)
top_trans_pin_dict = {
                        'State': [], 'Year': [], 'Quarter': [], 'Pincode': [],
                        'Transaction_count': [], 'Transaction_amount': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for regional_data in df['data']['pincodes']:
                    
                    name = regional_data['entityName']
                    count = regional_data['metric']['count']
                    amount = regional_data['metric']['amount']
                    
                    # Appending to top_trans_pin_dict
                    
                    top_trans_pin_dict['State'].append(state)
                    top_trans_pin_dict['Year'].append(year)
                    top_trans_pin_dict['Quarter'].append(int(quarter.removesuffix('.json')))                    
                    top_trans_pin_dict['Pincode'].append(name)
                    top_trans_pin_dict['Transaction_count'].append(count)
                    top_trans_pin_dict['Transaction_amount'].append(amount)
            except:
                pass

top_trans_pin_df = pd.DataFrame(top_trans_pin_dict)
##########################################################################
state_path = state_directories[5]
state_list = os.listdir(state_path)
top_user_dist_dict = {
                        'State': [], 'Year': [], 'Quarter': [],
                         'Registered_users': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for district_data in df['data']['districts']:
                    
                    name = district_data['name']
                    count = district_data['registeredUsers']
                    
                    # Appending to top_user_dist_dict
                    
                    top_user_dist_dict['State'].append(state)
                    top_user_dist_dict['Year'].append(year)
                    top_user_dist_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    #top_user_dist_dict['District'].append(name.title().replace(' And', ' and').replace('andaman', 'Andaman'))
                    top_user_dist_dict['Registered_users'].append(count)
            except:
                pass

top_user_dist_df = pd.DataFrame(top_user_dist_dict)
########################################################################
state_path = state_directories[5]
state_list = os.listdir(state_path)
top_user_pin_dict = {
                        'State': [], 'Year': [], 'Quarter': [],
                        'Pincode': [], 'Registered_users': []
                        }

for state in state_list:
    year_path = state_path + '/' + state + '/'
    year_list = os.listdir(year_path)
    
    for year in year_list:
        quarter_path = year_path + year + '/'
        quarter_list = os.listdir(quarter_path)
        
        for quarter in quarter_list:
            json_path = quarter_path + quarter
            df = pd.read_json(json_path)
            
            try:
                for regional_data in df['data']['pincodes']:
                    
                    name = regional_data['name']
                    count = regional_data['registeredUsers']
                    
                    # Appending to top_user_pin_dict
                    
                    top_user_pin_dict['State'].append(state)
                    top_user_pin_dict['Year'].append(year)
                    top_user_pin_dict['Quarter'].append(int(quarter.removesuffix('.json')))
                    top_user_pin_dict['Pincode'].append(name)
                    top_user_pin_dict['Registered_users'].append(count)
            except:
                pass

top_user_pin_df = pd.DataFrame(top_user_pin_dict)
###########################################################################

df_list = [df for df in globals() if isinstance(globals()[df], pd.core.frame.DataFrame) and df.endswith('_df')]


#lat_long_df = pd.read_csv(r"Miscellaneous/dist_lat_long.csv")


##for df_name in df_list:
##    df = globals()[df_name]
##    if 'District' in df.columns:
##        df = pd.merge(df, lat_long_df, on=['State'], how='left')
##        globals()[df_name] = df
##################################################################################
##        def add_suffix_to_districts(df):
##            if 'District' in df.columns and 'State' in df.columns:
##                delhi_df = df[df['State'] == 'Delhi']
##        
##                districts_to_suffix = [d for d in delhi_df['District'].unique() if d != 'Shahdara']
##        
##                df.loc[(df['State'] == 'Delhi') & (df['District'].isin(districts_to_suffix)), 'District'] = df.loc[(df['State'] == 'Delhi') & (df['District'].isin(districts_to_suffix)), 'District'].apply(lambda x: x + ' Delhi' if 'Delhi' not in x else x)
##                return df
##
##for df_name in df_list:
##    df = globals()[df_name]
##    add_suffix_to_districts(df)

###############################################################################
def add_region_column(df):
    state_groups = {
    'Northern Region': ['Jammu_and_Kashmir', 'Himachal_Pradesh', 'Punjab', 'Chandigarh', 'Uttarakhand', 'Ladakh', 'Delhi', 'Haryana'],
    'Central Region': ['Uttar_Pradesh', 'Madhya_Pradesh', 'Chhattisgarh'],
    'Western Region': ['Rajasthan', 'Gujarat', 'Dadra_and_Nagar_Haveli_and_Daman_and_Diu', 'Maharashtra'],
    'Eastern Region': ['Bihar', 'Jharkhand', 'Odisha', 'West_Bengal', 'Sikkim'],
    'Southern Region': ['Andhra_Pradesh', 'Telangana', 'Karnataka', 'Kerala', 'Tamil_Nadu', 'Puducherry', 'Goa', 'Lakshadweep', 'Andaman_and_Nicobar_Islands'],
    'North-Eastern Region': ['Assam', 'Meghalaya', 'Manipur', 'Nagaland', 'Tripura', 'Arunachal_Pradesh', 'Mizoram']
    }   
    df['Region'] = df['State'].map({state: region for region,states in state_groups.items() for state in states})
    df.fillna(0)
    return df
#######################################################################

for df_name in df_list:
    df = globals()[df_name]
    add_region_column(df)

#print('DATAFRAME INFO:\n')

for df_name in df_list:
    df = globals()[df_name]
    print(df_name + ':\n')
    #df.info()
    #print("\n", 45 * "_", "\n")
def save_dfs_as_csv(df_list):
    subfolder = 'Miscellaneous'
    if not os.path.exists(subfolder):
        os.makedirs(subfolder)
        
    for df_name in df_list:
        df = globals()[df_name]
        file_path = os.path.join(subfolder, df_name.replace('_df', '') + '.csv')
        df.to_csv(file_path, index=False)

# Calling function to execute

save_dfs_as_csv(df_list)

#################################################################
for df_name in df_list:
    df = globals()[df_name]
    print(f"{df_name}:")
    print(f"Null count: \n{df.isnull().sum()}")
    print(f"Duplicated rows count: \n{df.duplicated().sum()}")
    print(df.shape)
    print("\n", 25 * "_", "\n")

top_trans_pin_df.dropna(axis = 'index', inplace = True)
top_trans_pin_df.isnull().sum()
for df_name in df_list:
    df = globals()[df_name]
    df['Year'] = df['Year'].astype('int')

#print('OUTLIER COUNT ACROSS DATAFRAMES:\n')

##for df_name in df_list:
##    df = globals()[df_name]
##    outliers = count_outliers(df)
##    if len(outliers) == 0:
##        pass
##    else:
##        print(df_name, ":\n\n", outliers, "\n")
##        print("\n", 55 * "_", "\n")
        
############################################################
conn = mysql.connector.connect(
host = "localhost",
user = "root"
)
##############################################################
cursor = conn.cursor()

cursor.execute("DROP DATABASE IF EXISTS phonepe_pulse")

cursor.execute("CREATE DATABASE phonepe_pulse")

cursor.execute("USE phonepe_pulse")
#######################################################################
cursor.execute('''CREATE TABLE agg_trans (
                    State VARCHAR(255),
                    Year YEAR,
                    Quarter INTEGER,
                    Transaction_type VARCHAR(255),
                    Transaction_count INTEGER,
                    Transaction_amount FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year, Quarter, Transaction_type(255), Region(255))
                 )''')

cursor.execute('''CREATE TABLE agg_user (
                    State VARCHAR(255),
                    Year YEAR,
                    Quarter INTEGER,
                    Brand VARCHAR(255),
                    Transaction_count INTEGER,
                    Percentage FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year, Quarter, Brand(255), Region(255))
                 )''')

#cursor.execute('''CREATE TABLE map_trans (
#                    State VARCHAR(255),
#                    Year YEAR,
#                    Quarter INTEGER,
#                    Transaction_count INTEGER,
#                    Transaction_amount FLOAT,
#                    Latitude FLOAT,
#                    Longitude FLOAT,
#                    Region VARCHAR(255),
#                    PRIMARY KEY (State(255), Year, Quarter, Region(255))
#                 )''')

##cursor.execute('''CREATE TABLE map_user (
##                    State VARCHAR(255),
##                    Year YEAR,
##                    Quarter INTEGER,
##                    Registered_users INTEGER,
##                    App_opens INTEGER,
##                    Latitude FLOAT,
##                    Longitude FLOAT,
##                    Region VARCHAR(255),
##                    PRIMARY KEY (State(255), Year, Quarter, Region(255))
##                 )''')

##cursor.execute('''CREATE TABLE top_trans_dist (
##                    State VARCHAR(255),
##                    Year YEAR,
##                    Quarter INTEGER,
##                    Transaction_count INTEGER,
##                    Transaction_amount FLOAT,
##                    Latitude FLOAT,
##                    Longitude FLOAT,
##                    Region VARCHAR(255),
##                    PRIMARY KEY (State(255), Year, Quarter, Region(255))
##                 )''')

cursor.execute('''CREATE TABLE top_trans_pin (
                    State VARCHAR(255),
                    Year YEAR,
                    Quarter INTEGER,
                    Pincode VARCHAR(255),
                    Transaction_count INTEGER,
                    Transaction_amount FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year, Quarter, Pincode(255), Region(255))
                 )''')

cursor.execute('''CREATE TABLE top_user_dist (
                    State VARCHAR(255),
                    Year YEAR,
                    Quarter INTEGER,
                    Registered_users INTEGER,
                    Latitude FLOAT,
                    Longitude FLOAT,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year, Quarter, Region(255))
                 )''')

cursor.execute('''CREATE TABLE top_user_pin (
                    State VARCHAR(255),
                    Year YEAR,
                    Quarter INTEGER,
                    Pincode VARCHAR(255),
                    Registered_users INTEGER,
                    Region VARCHAR(255),
                    PRIMARY KEY (State(255), Year, Quarter, Pincode(255), Region(255))
                 )''')

##
##
dfs = {
    'agg_trans': agg_trans_df,
    'agg_user': agg_user_df,
    #'map_trans': map_trans_df,
#    'map_user': map_user_df,
#    'top_trans_dist': top_trans_dist_df,
    'top_trans_pin': top_trans_pin_df,
#    'top_user_dist': top_user_dist_df,
    'top_user_pin': top_user_pin_df
}
# Mapping table name to associated columns for each table

table_columns = {
    'agg_trans': list(agg_trans_df.columns),
    'agg_user': list(agg_user_df.columns),
#    'map_trans': list(map_trans_df.columns),
#    'map_user': list(map_user_df.columns),
#    'top_trans_dist': list(top_trans_dist_df.columns),
    'top_trans_pin': list(top_trans_pin_df.columns),
#    'top_user_dist': list(top_user_dist_df.columns),
    'top_user_pin': list(top_user_pin_df.columns)
}


def push_data_into_mysql(conn, cursor, dfs, table_columns):
    for table_name in dfs.keys():
        df = dfs[table_name]
        columns = table_columns[table_name]
        
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        for _, row in df.iterrows():
            data = tuple(row[column] for column in columns)
#            print(data)
            cursor.execute(query, data)
        conn.commit()
    print("Data successfully pushed into MySQL tables")

#################################################################
##
##
##
push_data_into_mysql(conn, cursor, dfs, table_columns)
##
#######################################################################
##
##
