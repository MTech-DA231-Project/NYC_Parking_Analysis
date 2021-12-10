from pyspark.sql.functions import col
import matplotlib.pyplot as plt

def violating_precicts(nyc_data, enable_plot=True):
    nyc_precints = nyc_data.select('violation_precinct')\
                           .groupBy('violation_precinct')\
                           .agg({'violation_precinct':'count'})\
                           .sort('count(violation_precinct)', ascending=False)
    return nyc_precints.toPandas()

def issuing_precincts(nyc_data, enable_plot=True):
    nyc_precints = nyc_data.select('issuer_precinct')\
                           .groupBy('issuer_precinct')\
                           .agg({'issuer_precinct':'count'})\
                           .sort('count(issuer_precinct)', ascending=False)
    return nyc_precints.toPandas()

def violation_code_frequency_top3_precincts(nyc_data, enable_plot=True):
    top3_precints = nyc_data.select('issuer_precinct')\
                           .groupBy('issuer_precinct')\
                           .agg({'issuer_precinct':'count'})\
                           .sort('count(issuer_precinct)', ascending=False)\
                           .take(3)
    top3 = [row['issuer_precinct'] for row in top3_precints]
    filtered_data = nyc_data.filter((col('issuer_precinct') == top3[0]) | (col('issuer_precinct') == top3[1]) | (col('issuer_precinct') == top3[2]))
    violation_frequencies_df = filtered_data.select('violation_code')\
                                         .groupBy('violation_code')\
                                         .agg({'violation_code':'count'})\
                                         .withColumnRenamed('count(violation_code)', 'Freq of Violations')\
                                         .sort('Freq of Violations', ascending=False)
          
    violation_frequencies = violation_frequencies_df.collect()

    if enable_plot:
        violations = [row['violation_code'] for row in violation_frequencies]
        frequencies = [row['Freq of Violations'] for row in violation_frequencies]

        fig, ax = plt.subplots(1, 1, figsize=(5,5))
        ax.set_title("Violations Vs Frequencies")
        ax.set_xlabel("Violations")
        ax.set_ylabel("Frequencies")
        ax.bar(violations[:5], frequencies[:5])

        fig.savefig('../output/violation_code_frequency_top3_precincts.png')

    return violation_frequencies_df.toPandas()
