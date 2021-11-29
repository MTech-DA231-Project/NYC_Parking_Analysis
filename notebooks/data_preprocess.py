# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # NYC Parking Violation Data Analysis
# %% [markdown]
# ## Downloading
# - Download CSV file only. Don't download CSV for excel (https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2022/pvqr-7yc4)
# - NY site has an option to visualize the data (https://data.cityofnewyork.us/d/kvfd-bves/visualization)
# %% [markdown]
# ## Columns Description
#
# | Source   Column Name    | Description/Comment                                  |
# |-------------------------|------------------------------------------------------|
# | SUMMONS-NUMBER          | UNIQUE IDENTIFIER OF SUMMONS                         |
# | PLATE ID                | REGISTERED PLATE   ID                                |
# | REGISTRATION   STATE    | STATE OF PLATE   REGISTRATION                        |
# | PLATE TYPE              | TYPE OF PLATE                                        |
# | ISSUE-DATE              | ISSUE DATE                                           |
# | VIOLATION-CODE          | TYPE OF   VIOLATION                                  |
# | SUMM-VEH-BODY           | VEHICLE BODY TYPE   WRITTEN ON SUMMONS (SEDAN, ETC.) |
# | SUMM-VEH-MAKE           | MAKE OF CAR WRITTEN   ON SUMMONS                     |
# | VIOLATION   PRECINCT    | PRECINCT OF   VIOLATION                              |
# | ISSUER   PRECINCT       | PRECINCT OF   ISSUANCE                               |
# | VEHICLE COLOR           | CAR COLOR WRITTEN ON   SUMMONS                       |
# | VIOLATION   DESCRIPTION | DESCRIPTION OF   VIOLATION                           |
# %% [markdown]
# ### Plate Type
#
# Registration Class Codes for vehicles. 3 letters code
#
# Common Plate types are
# * Passenger Vehicles (PAS): standard issue plates
# * Commercial Vehicles (COM): Full-size vans and most pickups
# * Medallion (OMT): Taxis
# * Personalized Plates (SRF): cars, mini-vans, SUVs and some pick-ups registered as passenger class
# * Special Omnibus Rentals (OMS)
#
# https://dmv.ny.gov/registration/registration-class-codes
#
# ### Violation Code
# Type of violation. Codes are from 1-99. Fines are charged based on this
#
# https://data.cityofnewyork.us/api/views/pvqr-7yc4/files/7875fa68-3a29-4825-9dfb-63ef30576f9e?download=true&filename=ParkingViolationCodes_January2020.xlsx
#
# ### Vehicle Body Type
#
# Common Vehicle body types are
# * suburban(SUBN): Vehicle that can be used to carry passengers and cargo
# * four-door sedan (4DSD)
# * Van Truck (VAN
# * Delivery Truck (DELV)
# * Pick-up Truck (PICK)
# * two-door sedan (2DSD)
# * Sedan (SEDN)
#
# https://nysdmv.custhelp.com/app/answers/detail/a_id/491/kw/body%20type%20subn
#
# ### Vehicle Make
#
# The DMV code for the make of a vehicle that appears on the registration. The DMV make code is the first 5 letters of the vehicle’s make name. If the vehicle make is more than one word, the make code is the first 2 letters of the first two words with a slash in between
#
# Common Vehicle Makes are
# * Honda (HONDA)
# * Toyota (TOYOT)
# * Ford (FORD)
# * Nissan (NISSA)
# * Chevrolet (CHEVR)
# * mercedes benz (ME/BE)
#
# https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i
# https://data.ny.gov/api/assets/83055271-29A6-4ED4-9374-E159F30DB5AE
#
# ### Vehicle Colors
#
# Common colors are
# * Gray (GY)
# * White (WH)
# * Black (BK)
# * Blue (BL)
# * Red (RD)
# %% [markdown]
# ## Config

# %%
# Use Sample file for speedy execution
sample_file = True
sample_file_path = "../data/sample-100000.csv"

# For faster execution. Some statements are skipped based on this check
presenting = False

# Specify the years for which we are reading the data from CSV
years = [2017, 2018, 2019, 2020, 2021]

# Schema Types. Only specify for the not-string type & NULL columns. Others  are considered as string
schema_types = {
  "Summons Number": {"type": "long", "null": False},
  "Issue Date"    : {"type": "date" if sample_file else "string", "null": True},
  "Violation Code": {"type": "integer", "null": True},
  "Violation Precinct": {"type": "integer", "null": True},
  "Issuer Precinct": {"type": "integer", "null": True},
}

# Columns which are used in the analysis. Other columns are removed
used_columns = ["Summons Number", "Plate ID", "Registration State", "Plate Type", "Issue Date", "Violation Code", "Vehicle Body Type", "Vehicle Make", "Violation Precinct", "Issuer Precinct", "Violation Time", "Vehicle Color", "Violation Description"]

# All the columns which are there in the datset (Need to be in CSV file order)
schema_columns = ["Summons Number", "Plate ID", "Registration State", "Plate Type", "Issue Date", "Violation Code", "Vehicle Body Type", "Vehicle Make", "Issuing Agency", "Street Code1", "Street Code2", "Street Code3", "Vehicle Expiration Date", "Violation Location", "Violation Precinct", "Issuer Precinct", "Issuer Code", "Issuer Command", "Issuer Squad", "Violation Time", "Time First Observed", "Violation County", "Violation In Front Of Or Opposite", "House Number", "Street Name", "Intersecting Street", "Date First Observed", "Law Section", "Sub Division", "Violation Legal Code", "Days Parking In Effect    ", "From Hours In Effect", "To Hours In Effect", "Vehicle Color", "Unregistered Vehicle?", "Vehicle Year", "Meter Number", "Feet From Curb", "Violation Post Code", "Violation Description", "No Standing or Stopping Violation", "Hydrant Violation", "Double Parking Violation"] if not sample_file else [i.lower().replace(" ", '_') for i in used_columns]

# Specify the CSV files path
csv_files = sample_file_path if sample_file else "../data/*.csv"

# Generates the sample CSV
if not sample_file:
  sample_CSV_generate = True # Generate the sample CSV
  sample_CSV_records = 100000 # No. of records to write into the sample CSV file
  sample_CSV_path = f"../data/sample-{sample_CSV_records}.csv" # path to save
  sample_seed = sample_CSV_records # Seed value so that we get same random records

# %% [markdown]
# ### Removing the columns which are not used in our analysis

# %%
def remove_unused_columns(df, used_columns):

  # Not used columns
  # "Issuing Agency", "Street Code1", "Street Code2", "Street Code3", "Vehicle Expiration Date", "Violation Location", "Issuer Code", "Issuer Command", "Issuer Squad", "Time First Observed", "Violation County", "Violation In Front Of Or Opposite", "House Number", "Street Name", "Intersecting Street", "Date First Observed", "Law Section", "Sub Division", "Violation Legal Code", "Days Parking In Effect    ", "From Hours In Effect", "To Hours In Effect", "Unregistered Vehicle?", "Vehicle Year", "Meter Number", "Feet From Curb", "Violation Post Code", "No Standing or Stopping Violation", "Hydrant Violation", "Double Parking Violation"

  all_columns = set(df.columns)
  used_columns = set(used_columns)
  unused_columns = all_columns - used_columns

  df = df.drop(*unused_columns)

  print(f'No. of Columns (before dropping columns) : {len(all_columns)}')
  print(f'No. of Columns (after dropping columns) : {len(df.columns)}')
  print(f'No. of Rows : {df.count()}')

  return df

# %% [markdown]
# ### Dropping Duplicate Rows

# %%
def drop_duplicates(df):

  print(f'No. of Records (before dropping duplicates) : {df.count()}')

  df = df.drop_duplicates()

  print(f'No. of Records (after dropping duplicates) : {df.count()}')

  return df

# %% [markdown]
# ### Converting column names to lower case & replacing spaces with _

# %%
def santize_column_names(df):

  print(f'Columns (before sanitizing column names) : {df.columns}')

  ## Slow
  # columns = [column.lower().replace(" ", "_") for column in df.columns ]
  # df = df.toDF(*columns)

  df = df.select([col(c).alias(c.lower().replace(" ", '_')) for c in df.columns])

  print(f'Columns (after sanitizing column names) : {df.columns}')

  return df

# %% [markdown]
# ### Ensure all the values in in a column are unique

# %%
def assert_uniqueness(df, column_name):

  all_rows      = df.select(column_name).count()
  distinct_rows = df.select(column_name).distinct().count()

  assert all_rows == distinct_rows

  print(f"All values in {column_name} column are unique")

  return True

# %% [markdown]
# ### Converting issue date string type to Date type

# %%
def convert_to_date(df, column_name, format):

  df = (
        df
        .withColumn(
          column_name,
          F.to_date(col(column_name), format)
        )
      )

  return df

# %% [markdown]
# ### Removing the rows which are outside of the passed years

# %%
print(df.select(F.year(col("issue_date"))).distinct().count())
df.select(F.year(col("issue_date"))).distinct().show()

# Records are spanned over 24 years


# %%
def remove_outside_years_data(df, years, column_name):

  print(f'Distinct years in {column_name} (before removing) : {df.select(F.year(col(column_name))).distinct().show()}')

  min_year = min(years)
  max_year = max(years)

  df = (
        df
        .select('*')
        .where(
          (F.year(col(column_name)) >= min_year)
          &
          (F.year(col(column_name)) <= max_year)
        )
      )

  print(f'Distinct years in {column_name} (after removing) : {df.select(F.year(col(column_name))).distinct().show()}')

  return df


# %%
df.select("violation_code").distinct().count()


# %%
def remove_invalid_violation_code_data(df):

  print(f'Distinct violation codes (before removing) : {df.select("violation_code").distinct().count()}')

  df = (
        df
        .select('*')
        .where((col("violation_code") >= 1) & (col("violation_code") <= 99))
      )

  print(f'Distinct violation codes (before removing) : {df.select("violation_code").distinct().count()}')

  return df

# %% [markdown]
# ## Creating Spark session

# %%
import findspark
findspark.init()
findspark.find()

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col # Frequently using this. hence imported separately

spark = (
          SparkSession
            .builder
            .master("local[4]") # Using 4 cores
            .appName("NY_Parking_violation")
            .config('spark.ui.port', '4050')
            .getOrCreate()
        )
spark


# %%
# spark.stop() #TODO: Use this at the end of th script

# %% [markdown]
# ## Reading CSV files into DataFrame

# %%
def get_schema(schema_columns, schema_types):
  schema = []
  for col in schema_columns:
    schema_str = f"`{col}` "
    if col in schema_types:
      schema_str += f"{schema_types[col]['type']} "
      schema_str += "NOT NULL" if not schema_types[col]['null'] else ""
    else:
      schema_str += "string"
    schema.append(schema_str)

  return ','.join(schema)


# %%
# Reading the CSV into data frame

# Better performance than infer Schema True
NY_schema = get_schema(schema_columns, schema_types)

org_df = spark.read.option("header", True).schema(NY_schema).csv(csv_files)


# %%
print(f'Shape : {(org_df.count(), len(org_df.columns))}')
org_df.printSchema()
org_df.show(2)


# %%
presenting and org_df.summary().show() # More execution time

# %% [markdown]
# ## Pre-processing

# %%
# No pre-processing while using Sample file
df = org_df
if not sample_file:
  df = remove_unused_columns(df, used_columns)
  df = drop_duplicates(df)
  df = santize_column_names(df)
  assert_uniqueness(df, column_name="summons_number")
  df = convert_to_date(df, column_name="issue_date", format="MM/dd/yyyy")
  df = remove_outside_years_data(df, years, "issue_date")
  df = remove_invalid_violation_code_data(df)


# %%
if not sample_file:
  total_records = df.count()
  print(f'Shape : {(total_records, len(df.columns))}')
  df.printSchema()
  df.show(2)


# %%
presenting and df.describe().show() # More execution time

# %% [markdown]
# ## Saving Sample

# %%
from pathlib import Path
from shutil import rmtree

def write_CSV(df, CSV_path):

  # Creates CSV in a folder. But memory efficient
  df.coalesce(1).write.mode("overwrite").csv(CSV_path, header=True)

  # Moving file to data folder
  f_path = list(Path(CSV_path).glob('*.csv'))[0]
  Path(f_path).rename(CSV_path+'.tmp')
  rmtree(CSV_path)
  Path(CSV_path+'.tmp').rename(CSV_path)

  # df.toPandas().to_csv(CSV_path, index=False)


# %%
if (not sample_file and sample_CSV_generate):
  fraction = (sample_CSV_records+10000)/total_records # Exact records are not coming. Hence increasing the fraction using 10k
  sample_df = df.sample(fraction=fraction, seed=sample_seed).limit(sample_CSV_records)
  print(sample_df.count())
  write_CSV(df, sample_CSV_path)


# %%



