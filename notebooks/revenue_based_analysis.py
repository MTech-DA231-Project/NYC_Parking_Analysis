from pyspark.sql.functions import count, desc
import pandas as pd
import matplotlib.pyplot as plt

def yearly_revenue(data_frame, enable_plot=True):
    years = ["2017", "2018", "2019", "2020", "2021"]
    revenue = []
    
    for year in years:
        violation_count = get_violation_df__yearly(data_frame, year)
        revenue.append(accumulatedTax_per_Violation(violation_count)['cost'].sum())

    if enable_plot:
        fig, ax = plt.subplots(1, 1, figsize=(5,5))
        ax.set_title("Year Vs Revenue in $")
        ax.set_xlabel("Year")
        ax.set_ylabel("Revenue in $")
        ax.bar(years, revenue)

        fig.savefig('../output/yearly_revenue.png')
    
    year_revenue = pd.DataFrame(revenue, columns = ['Revenue in $'], index=years)
    year_revenue.index.name = 'Year'

    return year_revenue

def highest_revenue(data_frame, enable_plot=True):
    violation_count =  get_violation_df__yearly(data_frame, "")
    top10_revenues = accumulatedTax_per_Violation(violation_count).head(10)
    
    if enable_plot:
      ax = top10_revenues.plot.bar(x='violation_code', y='cost', figsize=(10, 5))
      ax.set_title("Violation code vs Revenue")
      ax.set_xlabel("Violation code")
      ax.set_ylabel("Revenue")
      fig = ax.get_figure()
      fig.savefig('../output/highest_revenue.png')

    return top10_revenues

def calulateTax(violation_code, frequency, dict_map):
    price_rate = dict_map.get(int(violation_code))
    return price_rate * frequency

def accumulatedTax_per_Violation(df):
    df = df.toPandas()
    price_df = read_priceTag_violation_csv()
    dic_map = dict(zip(price_df.violation_code, price_df.Fine))
    df['cost'] = df.apply(lambda x: calulateTax(1, x['no_of_tickets'], dic_map), axis=1)
    df = df[['violation_code', 'cost']]
    sorted_df = df.sort_values(by=['cost'], ascending=False)
    return sorted_df

def read_priceTag_violation_csv():
    price_df = pd.read_excel(r'../docs/ParkingViolationCodes_January2020.xlsx')
    dict = {'VIOLATION CODE': 'violation_code','All Other Areas\n(Fine Amount $)': 'Fine'}
    price_df.rename(columns=dict, inplace=True)
    return price_df

def get_violation_df__yearly(df, year):
    return df.select('violation_code', 'issue_date')\
        .filter(df['issue_date'].rlike(year + "-*"))\
        .groupBy('violation_code')\
        .agg(count('violation_code').alias('no_of_tickets'))\
        .sort(desc('no_of_tickets'))
