######################
###Import Libraries###
######################

from numpy.core.numeric import NaN
import pandas as pd
import geopy
import time


######################
###Define Functions###
######################

def import_data(file_location):
    '''Import the data and append to one dataframe. Keep data that occurs after Bottoms was elected.'''
    df  = pd.DataFrame(columns = keep_cols)
    for datafile in file_location:
        data = pd.read_csv(datafile, parse_dates = ['rpt_date'],low_memory = False)
        data = data[keep_cols]
        data = data[(data['rpt_date'] > pd.Timestamp(2018,1,1))]
        df = df.append(data, ignore_index = True)
    df.to_csv(export_crime)
    return df

def get_geodata(df, geolocator, lat_field, lon_field):
    '''Use Latitude and Longitude to find the Zip Codes'''
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    return location.raw

def create_geo_file(df):
    '''For each row in the df, run get_geodata and append to new match_df. If zipcode is missing from get_geodata then skip row'''
    match_df = pd.DataFrame(columns = match_cols)
    for row in range(1,len(df.index)):
        geo_object = df[row-1:row].apply(get_geodata, axis=1, geolocator=geolocator, lat_field='lat', lon_field='long')
        for index, value in geo_object.items():
            try:
                temp_df = pd.DataFrame([[value['lat'], value['lon'], value['address']['postcode']]], columns = match_cols)
            except:
                temp_df = pd.DataFrame([[value['lat'], value['lon'], None]], columns = match_cols)
            match_df = match_df.append(temp_df, ignore_index = True)
            print(match_df.tail(n=1))
    match_df.to_csv(export_match)
    return match_df

def merge_export_df(df1,df2):
    '''Merge the original and match dataframes then export'''
    df2['lat'] = pd.to_numeric(df2['lat'])
    df2['long'] = pd.to_numeric(df2['long'])
    match_geo_df = pd.merge(df1, df2, how="inner", left_index=True, right_index=True, suffixes=("_orig", "_match"))
    match_geo_df.to_csv(export_file)


######################
####Create objects####
######################

#list files
file1 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2021.csv"
file2 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020(NEW RMS 9-30 12-31).csv"
file3 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020-OldRMS-09292020.csv"
file4 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv"
file_list = [file1, file2, file3, file4]
export_crime = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime.csv"
export_match = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\match.csv"
export_file = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"

#Columns to keep in original dataframe
keep_cols = ['rpt_date', 'location', 'UC2_Literal', 'neighborhood', 'lat', 'long']

#Columns to keep in matching dataframe
match_cols = ['lat', 'long', 'zipcode']

#Establish Connection to geopy mechanism
geolocator = geopy.Nominatim(user_agent='boob')


#####################
####Run functions####
#####################

def main():

    #Import Data
    crime_df = import_data(file_list)
    
    #Retrieve GeoData
    match_df = create_geo_file(crime_df)

    #Merge the dataframes the export
    merge_export_df(crime_df, match_df)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))