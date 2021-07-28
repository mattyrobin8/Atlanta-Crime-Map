######################
###Import Libraries###
######################

from numpy.core.numeric import NaN
import pandas as pd
import pandasql as ps
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
        data = data[(data['rpt_date'] >= pd.Timestamp(2000,1,1))]
        df = df.append(data, ignore_index = True)
    df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)
    df['Crime'] = df['Crime'].replace(['MANSLAUGHTER'],'HOMICIDE')
    return df


######################
####Create objects####
######################

#Import files
file1 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2021.csv"
file2 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020(NEW RMS 9-30 12-31).csv"
file3 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2020-OldRMS-09292020.csv"
file4 = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Data\COBRA-2009-2019.csv"
file_list = [file1, file2, file3, file4]
export_file = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\kasim_reed.csv"

#Columns to keep in original dataframe
keep_cols = ['rpt_date', 'location', 'UC2_Literal', 'neighborhood', 'lat', 'long']


######################
####Create queries####
######################

#Create ATL crime dataframe
crime_query = 	"""
				select  	strftime('%Y', rpt_date) as year
							,count(*) as total_crime
				from 		crime_df
				group by 	year
				order by	year
				"""


#####################
####Run functions####
#####################

def main():

    #Import Data
    crime_df = import_data(file_list)

    #Run ATL crime query
    atlcrime_df = ps.sqldf(crime_query)
    
    #Export the data
    atlcrime_df.to_csv(export_file, index=False)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))