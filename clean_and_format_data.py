import sqldf
import pandasql as ps


df[['Crime','Crime Extra']] = df.UC2_Literal.str.split("-",expand=True)

query = """
			select  strftime('%Y', rpt_date) as year
					,Crime
					,count(*) as crime
			from crime_df
			group by Crime
					 ,strftime('%Y', rpt_date)
		"""
print(ps.sqldf(query))