import pandas as pd

def sort_and_reindex_positions_by_date(input_file, output_file):
    """
    Sorts a CSV file by the 'date' column (dd mmm yyyy format) and re-indexes the 'index' column.

    Args:
        input_file (str): The path to the input CSV file.
        output_file (str): The path to save the modified CSV file.
    """
    try:
        # Load the CSV file into a pandas DataFrame
        df = pd.read_csv(input_file)

        # Validate if 'date' and 'index' columns exist
        if 'date' not in df.columns:
            raise ValueError("The 'date' column is not found in the CSV file.")
        if 'index' not in df.columns:
            raise ValueError("The 'index' column is not found in the CSV file.")

        # Convert 'date' column to datetime objects using the specified format
        # '%d %b %Y' corresponds to 'DD Mon YYYY' (e.g., '25 Jul 2025')
        # errors='coerce' will convert any unparseable dates to NaT (Not a Time)
        df['date'] = pd.to_datetime(df['date'], format='%d %b %Y', errors='coerce')

        # Drop rows where date conversion failed (if any).
        # This handles cases where the date format might not be consistent.
        initial_rows = len(df)
        df.dropna(subset=['date'], inplace=True)
        if len(df) < initial_rows:
            print(f"Warning: {initial_rows - len(df)} rows were dropped due to unparseable dates.")


        # Sort the DataFrame by the 'date' column
        df_sorted = df.sort_values(by='date', ascending=True)

        # Reset the 'index' column to incremental numbers
        # We use range(1, len(df_sorted) + 1) to start the index from 1
        df_sorted['index'] = range(1, len(df_sorted) + 1)

        # Convert the 'date' column back to the original 'dd mmm yyyy' string format for saving
        df_sorted['date'] = df_sorted['date'].dt.strftime('%d %b %Y')

        # Save the updated DataFrame to a new CSV file
        df_sorted.to_csv(output_file, index=False)

        print(f"File '{input_file}' has been sorted by actual date and indexed incrementally.")
        print(f"The modified data is saved to '{output_file}'.")

    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
    except ValueError as e:
        print(f"Data Validation Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# How to use the function:
# Assuming 'positions.csv' is in the same directory as your script
input_csv_file = 'positions.csv'
output_csv_file = 'positions_sorted_by_date_indexed.csv'
sort_and_reindex_positions_by_date(input_csv_file, output_csv_file)
