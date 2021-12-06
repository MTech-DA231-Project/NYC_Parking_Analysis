from pyspark.sql.functions import count,desc
import matplotlib.pyplot as plt


def violation_frequencey(data_frame):
    violation_count = 0
    violation_count = data_frame.select(data_frame['Violation Code'].alias('Violation_Code'))\
                              .groupBy('Violation_Code')\
                              .agg(count('Violation_Code')\
                              .alias('no_of_tickets'))\
                              .sort(desc('no_of_tickets'))
    # plot top 5 Code Violation
    q3_for_plot = violation_count.toPandas()
    plt.clf()
    q3_for_plot.head(5).plot(x='Violation_Code', y='no_of_tickets', kind='bar')
    plt.show()
    return violation_count

def violations_by_bodytype(data_frame):
    body_type = 0
    body_type  = data_frame.select(data_frame['Vehicle Body Type'].alias('Vehicle_Body_Type'))\
                              .groupBy('Vehicle_Body_Type')\
                              .agg(count('Vehicle_Body_Type')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Body_Type
    vehicleBodyType_for_plot = body_type.toPandas()
    plt.clf()
    vehicleBodyType_for_plot.head(5).plot(x='Vehicle_Body_Type', y='Ticket_Frequency', kind='bar')
    plt.title("Violations on the basis of Vehicle_Body_Type")
    plt.xlabel('Vehicle Body Type')
    plt.ylabel('Ticket Frequency')
    plt.show()                      
    return body_type

def violations_by_make(data_frame):
    make_type = 0
    make_type  = data_frame.select(data_frame['Vehicle Make'].alias('Vehicle_Make'))\
                              .groupBy('Vehicle_Make')\
                              .agg(count('Vehicle_Make')\
                              .alias('Ticket_Frequency'))\
                              .sort(desc('Ticket_Frequency'))
    # plot Violations on the basis of Vehicle_Make
    vehicleMake_for_plot = data_frame.toPandas()
    plt.clf()
    vehicleMake_for_plot.head(5).plot(x='Vehicle_Make', y='Ticket_Frequency', kind='bar')
    plt.title("Violations on the basis of Vehicle_Make")
    plt.xlabel('Vehicle Make')
    plt.ylabel('Ticket Frequency')
    plt.show()                      
    return make_type