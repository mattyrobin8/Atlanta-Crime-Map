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
	print(df['zipcode'].unique())
	df.loc[df['zipcode'] == '30031', 'zipcode'] = '300313'
	print(df['zipcode'].unique())
	return df


######################
####Create objects####
######################

#List files
file_location = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_with_zips.csv"
export_file = r"C:\Users\matty\OneDrive\Politics\Mayor Felicia\Out\crime_for_tableau.csv"

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

#Pull denominator for extrapolation of 2021 data
numerator_2021_query = """
				select  	year
							,Crime
							,sum(total_crime) as total_crime
				from 		atlcrime_df
				where		zipcode not in ('None')
				and			year = 2020
				group by	year
							,Crime
				order by 	year
				"""

#Pull numerator for extrapolation of 2021 data
denominator_2021_query = """
				select  	year
							,Crime
							,sum(total_crime) as total_crime
				from 		atlcrime_df
				where		zipcode not in ('None')
				and			month in ('01','02','03','04','05')
				and			year = 2020
				group by	year
							,Crime
				order by 	year
							,Crime
				"""

#Create index for 2021 extrapolation
index_2021_query = """
				select		num.Crime
							,avg(cast(num.total_crime as float) / cast(dem.total_crime as float)) as crime_index
				from 		numerator_2021_df num
				join		denominator_2021_df dem
				on			num.year = dem.year
				and			num.Crime = dem.Crime
				group by	num.Crime
				"""

#Apply 2021 crime index
crime_2021_query = """
				select 	year
						,zipcode
						,crime.Crime
						,cast(round(sum(total_crime) * crime_index,0) as int) as total_crime
				from atlcrime_df crime
				join index_2021_df ind
				on crime.Crime = ind.Crime
				where year = 2021
				group by year
						,zipcode
						,crime.Crime
				"""

#Pull 2020 crime data
crime_2020_query = """
				select 	year
						,zipcode
						,Crime
						,sum(total_crime) as total_crime
				from atlcrime_df
				where year not in (2021)
				group by year
						,zipcode
						,Crime
				"""

#Join ATL crime and population dataframes
crimepop_query = """
				select  	crime.year
							,zipcode
							,Crime
							,population
							,sum(total_crime) as total_crime
							,(sum(cast(total_crime as float)) / cast(population as float)) * 100000 as crimes_per_100k
				from 		crime_aggregated_df crime
				join		atlpop_df pop
				on			crime.year = pop.year
				group by 	zipcode
							,Crime
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

	#Run 2021 numerator
	numerator_2021_df = ps.sqldf(numerator_2021_query)

	#Run 2021 denominator
	denominator_2021_df = ps.sqldf(denominator_2021_query)

	#Run 2021 crime index query 
	index_2021_df = ps.sqldf(index_2021_query)

	#Run 2021 crime index 
	crime_2021_df = ps.sqldf(crime_2021_query)
	
	#Run 2020 Crime query
	crime_2020_df = ps.sqldf(crime_2020_query)

	#Append 2021 extrapolated to 2020 actuals
	crime_aggregated_df = crime_2020_df.append(crime_2021_df, ignore_index=True)

	#Run ATL population query
	atlcrimepop_df = ps.sqldf(crimepop_query)

	#Transform the data for easy pct change calculation
	actuals_df = pd.pivot_table(data=atlcrimepop_df, index=['Crime','zipcode'], columns=['year'], values=['total_crime'])
	actuals_df = actuals_df.reset_index()
	actuals_df.columns = ['crime','zipcode','total_crime_2018','total_crime_2019','total_crime_2020','total_crime_2021']
	percent_df = actuals_df.iloc[:, 2:].pct_change(axis='columns')

	#Merge and do percent change
	blarg = pd.merge(actuals_df, percent_df, how="inner", left_index=True, right_index=True, suffixes=("_actuals", "_pct_change"))

	#Export the data
	blarg.to_csv(export_file, index=False)

	honk = """
					select  	crime.year
								,crime
								,population
								,sum(total_crime) as total_crime
								,(sum(cast(total_crime as float)) / cast(population as float)) * 100000 as crimes_per_100k
					from 		crime_aggregated_df crime
					join		atlpop_df pop
					on			crime.year = pop.year
					group by	crime
								,crime.year
					order by 	crime
								,crime.year
					"""
	print(ps.sqldf(honk))

#Run Main script and record runtime
if __name__ == '__main__':
    start_time = time.time()
    main()
    print("--- %s seconds ---" % (time.time() - start_time))