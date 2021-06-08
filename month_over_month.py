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
	df = pd.read_csv(file_location, parse_dates = ['rpt_date'], low_memory = False)
	df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)
	df['Crime'] = df['Crime'].replace(['MANSLAUGHTER'],'HOMICIDE')
	df['zipcode'] = df['zipcode'].str[:5]
	df.loc[df['zipcode'] == '30031', 'zipcode'] = '300313'
	return df


######################
####Create objects####
######################

#List files
file_location = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"
export_file = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_for_tableau_month.csv"

#Create ATL population dataframe
data = [['2018',459600],['2019',470500],['2020',478200],['2021',(478200-470500) + 478200]]
atlpop_df = pd.DataFrame(data, columns = ['year', 'population'])


######################
####Create queries####
######################

#Create ATL crime dataframe
crime_query = 	"""
				select  	strftime('%Y', rpt_date) as year
							,strftime('%m', rpt_date) as month
							,strftime('%Y%m', rpt_date) as year_month
							,zipcode
							,Crime
							,count(*) as total_crime
				from 		crime_df
				where		zipcode not in ('None')
				group by 	Crime
							,zipcode
							,strftime('%Y', rpt_date)
							,strftime('%m', rpt_date)
							,strftime('%Y%m', rpt_date)
				order by	strftime('%Y%m', rpt_date)
							,zipcode
							,Crime
				"""

#Join ATL crime and population dataframes
crimepop_query = """
				select  	crime.year
							,zipcode
							,population
							,sum(total_crime) as total_crime
							,(sum(cast(total_crime as float)) / cast(population as float)) * 100000 as crimes_per_100k
				from 		atlcrime_df crime
				join		atlpop_df pop
				on			crime.year = pop.year
                where       month in ('01','02','03','04','05')
				group by 	zipcode
							,crime.year
							,population
				"""

#####################
####Run functions####
#####################

def main():

    #Import processed crime dataframe
	crime_df = import_data(file_location)

	#Run ATL crime query
	atlcrime_df = ps.sqldf(crime_query)

	#Run ATL population query
	atlcrimepop_df = ps.sqldf(crimepop_query)

	#Transform the data for easy pct change calculation
	#actuals_df = pd.pivot_table(data=atlcrimepop_df, index=['Crime','zipcode'], columns=['year'], values=['total_crime'])
	actuals_df = pd.pivot_table(data=atlcrimepop_df, index=['zipcode'], columns=['year'], values=['total_crime'])
	actuals_df = actuals_df.reset_index()
	#actuals_df.columns = ['crime','zipcode','total_crime_2018','total_crime_2019','total_crime_2020','total_crime_2021']
	actuals_df.columns = ['zipcode','total_crime_2018','total_crime_2019','total_crime_2020','total_crime_2021']
	percent_df = actuals_df.iloc[:, 2:].pct_change(axis='columns')

	#Merge and do percent change
	blarg = pd.merge(actuals_df, percent_df, how="inner", left_index=True, right_index=True, suffixes=("_actuals", "_pct_change"))

	#Export the data
	blarg.to_csv(export_file, index=False)

	honk = """
					select  	year
								,population
								,sum(total_crime) as total_crime
								,(sum(cast(total_crime as float)) / cast(population as float)) * 100000 as crimes_per_100k
					from 		atlcrimepop_df
					group by	year
					order by 	year
					"""
	print(ps.sqldf(honk))

#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))