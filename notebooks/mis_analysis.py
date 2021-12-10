#  in-state vs. out of state, impact of holidays/long weekends, repeat offenders, etc.
def repeat_offenders(nyc_data):
    repeat_offenders = nyc_data.select('plate_id')\
                               .dropna()\
                               .groupBy('plate_id')\
                               .agg({'plate_id': 'count'}).withColumnRenamed('count(plate_id)', 'No of Repeat violations')\
                               .sort('No of Repeat violations', ascending=False)

    offenders = repeat_offenders.toPandas()
    ax = offenders.head(10).plot.bar(x='plate_id', y='No of Repeat violations')
    ax.set_title("Affenders Vs No of violations")
    ax.set_xlabel("Affenders")
    ax.set_ylabel("Violations")
    fig = ax.get_figure()
    fig.savefig('../output/repeat_affenders.png')

    return offenders
