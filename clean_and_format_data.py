######################
###Import Libraries###
######################

from os import error
import pandas as pd
import pandasql as ps
import time


######################
###Define Functions###
######################

def import_data(file_location):
	'''Read in Crime Data'''
	df = pd.read_csv(file_location, low_memory = False)
	df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)
	return df


######################
####Create objects####
######################

#List files
file_location = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"


#####################
####Run functions####
#####################

def main():

    #Import and clean data
	crime_df = import_data(file_location)
	crime_df['year'] = pd.DatetimeIndex(crime_df['rpt_date']).year
	crime_df['month'] = pd.DatetimeIndex(crime_df['rpt_date']).month
	crime_df['year_month'] = pd.to_datetime(crime_df[['year','month']].assign(day=1)).dt.to_period('M')
	print(crime_df['rpt_date'].dtype)

	#Create ATL crime dataframe


	#Create ATL population dataframe
	data = [['2018',459600],['2019',470500],['2020',478200],['2021',(478200-470500) + 478200]]
	atlpop_df  = pd.DataFrame(data, columns = ['year', 'population'])
	#print(atlpop_df)


#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))