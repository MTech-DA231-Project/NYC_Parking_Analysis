from pyspark.sql.functions import count,desc
import matplotlib.pyplot as plt

def violation_frequencey(data_frame):
    #violation_count = 0
    violation_count = data_frame.select('violation_code')\
                                .groupBy('violation_code')\
                                .agg(count('violation_code')\
                                .alias('no_of_tickets'))\
                                .sort(desc('no_of_tickets'))
    # plot top 5 Code Violation
    q3_for_plot = violation_count.toPandas()
    plt.clf()
    q3_for_plot.head(5).plot(x='violation_code', y='no_of_tickets', kind='bar')
    plt.savefig('../output/violation_frequencey.png')
    return q3_for_plot

def violations_by_bodytype(data_frame):
    #body_type = 0
    body_type  = data_frame.select('vehicle_body_type')\
                              .groupBy('vehicle_body_type')\
                              .agg(count('vehicle_body_type')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Body_Type
    vehicleBodyType_for_plot = body_type.toPandas()
    plt.clf()
    vehicleBodyType_for_plot.head(5).plot(x='vehicle_body_type', y='Ticket_Frequency', kind='bar')
    plt.title("Violations on the basis of vehicle_body_type")
    plt.xlabel('Vehicle Body Type')
    plt.ylabel('Ticket Frequency')
    plt.savefig('../output/violations_by_bodytype.png')
    return vehicleBodyType_for_plot

def violations_by_make(data_frame):
    make_type  = data_frame.select('vehicle_make')\
                              .groupBy('vehicle_make')\
                              .agg(count('vehicle_make')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Make
    vehicleMake_for_plot = make_type.toPandas()
    plt.clf()
    vehicleMake_for_plot.head(5).plot(x='vehicle_make',  y='Ticket_Frequency', kind='bar')
    plt.title("Violations on the basis of vehicle_make")
    plt.xlabel('Vehicle Make')
    plt.ylabel('Ticket Frequency')
    plt.savefig('../output/violations_by_make.png')
    return vehicleMake_for_plot
