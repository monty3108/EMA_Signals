# import pandas as pd
#
# # Assume this is your DataFrame
# data = {'Event': ['Start', 'Midpoint', 'End'],
#         # If this column is read from a CSV, it's often a string ('object') dtype
#         'Date_Time': ['2025-01-15 10:30:00', '2025-05-20 14:00:00', '2025-10-30 09:15:00']}
# df = pd.DataFrame(data)
#
# # Check the initial data type (It likely shows 'object' or 'string', causing the error)
# print("Initial Data Type:", df['Date_Time'].dtype)
# print("-" * 40)
#
# # ----------------------------------------------------
# # **STEP 1: Fix the Error by Converting the Column**
# # ----------------------------------------------------
# # You must explicitly convert the column to the datetime format first.
# df['Date_Time'] = pd.to_datetime(df['Date_Time'])
#
# # Verify the conversion (It should now show 'datetime64[ns]')
# print("Data Type after Conversion:", df['Date_Time'].dtype)
# print("-" * 40)
#
# # ----------------------------------------------------
# # **STEP 2: Apply .dt.strftime() (This will now work!)**
# # ----------------------------------------------------
# # Now that the column is a proper datetime type, you can use the .dt accessor.
# df['Formatted_Date_Str'] = df['Date_Time'].dt.strftime('%d %b %Y')
#
# print("Resulting DataFrame with Formatted String Column:")
# print(df[['Event', 'Formatted_Date_Str']])
#
# print("Data Type after strf:", df['Formatted_Date_Str'].dtype)

from datetime import datetime
import pandas as pd
import csv

# 1. Start with a <class 'datetime.datetime'> object
original_datetime = datetime.now() # Creates a datetime object for the current moment

print(f"Original Object Type: {type(original_datetime)}")
print(f"Original Value: {original_datetime}")
print("-" * 40)

# 2. **Core Action:** Convert the datetime object to a string
# The format codes are:
# %d: Day of the month (01-31)
# %b: Month as locale’s abbreviated name (e.g., Oct)
# %Y: Year with century (e.g., 2025)
DATE_FORMAT = '%d %b %Y'
formatted_date_string = original_datetime.strftime(DATE_FORMAT)

print(f"Formatted String: {formatted_date_string}")
print(f"Formatted Type: {type(formatted_date_string)}")