#  in-state vs. out of state, impact of holidays/long weekends, repeat offenders, etc.
def repeat_offenders(spark):

  repeat_offenders = spark.sql("\
                                select plate_id, count(plate_id) as no_of_violations \
                                from NYCPV \
                                group by plate_id \
                                order by no_of_violations desc"
                              )

  offenders = repeat_offenders.toPandas()

  ax = offenders.head(10).plot.bar(x='plate_id', y='no_of_violations', figsize=(12, 8))
  ax.set_title("Plate ID vs No. of violations")
  ax.set_xlabel("Plate ID")
  ax.set_ylabel("Violations")
  fig = ax.get_figure()
  fig.savefig('../output/repeat_offenders.png')

  return offenders


def in_out_state(spark):

  in_out_state = spark.sql("\
                            select registration_state, count(registration_state) as no_of_violations \
                            from NYCPV \
                            group by registration_state \
                            order by no_of_violations desc"
                          )

  in_out_state_PD = in_out_state.toPandas()

  ax = in_out_state_PD.head(10).plot.bar(x='registration_state', y='no_of_violations', figsize=(12, 8))
  ax.set_title("State vs No. of violations")
  ax.set_xlabel("State")
  ax.set_ylabel("Violations")
  fig = ax.get_figure()
  fig.savefig('../output/in_out_state.png')

  return in_out_state_PD
