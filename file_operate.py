import os
import pandas as pd
import datetime as dt
import time

import config

# from My_Logger import setup_logger, LogLevel
# logger = setup_logger(logger_name="File Operate", log_level=LogLevel.INFO, log_to_console=config.print_logger)


date_format = config.date_format
filepath_positions = config.filepath_positions

def print_android(str):
    space = 4 * " "
    print(f'{space}{str}')

def clear_console():
    """
    Clears the console screen based on the operating system.
    """
    # For Windows
    if os.name == 'nt':
        _ = os.system('cls')
    # For macOS and Linux (posix refers to POSIX compliant systems)
    else:
        _ = os.system('clear')

def file_operate(filepath='positions.csv'):
    """
    Operates on the 'positions.csv' file to modify existing entries,
    add new entries, view all records, and save changes.

    Args:
        filepath (str): The path to the CSV file.
    """
    import pandas as pd
    import numpy as np

    # Initial check
    def initial_check(file_name=filepath_positions, date_format=config.date_format):
        # Initial check 1
        def check_date_format(file_path: str, date_format: str):
            """
                    Checks the 'date' column against the specified format.
                    Identifies and reports rows with incorrect date formats,
                    explaining where interactive input would be taken in a local environment.
                    """
            print(f"--- Checking dates in '{file_path}' against format '{date_format}' ---")
            # Load the CSV. Read the index column correctly.
            try:
                df = pd.read_csv(file_path, index_col='index')
            except FileNotFoundError:
                print(f"Error: File '{file_path}' not found.")
                return

            # Attempt to parse dates, coercing errors to NaT
            df['validated_date'] = pd.to_datetime(df['date'], format=config.date_format, errors='coerce')

            # Identify rows where conversion failed (i.e., NaT)
            incorrect_dates = df[df['validated_date'].isnull()]
            if not incorrect_dates.empty:
                print("\n--- ❌ INCORRECT DATES FOUND ---")
                # Iterate over the rows with bad dates to 'prompt' the user
                for index, row in incorrect_dates.iterrows():
                    print(incorrect_dates)
                    original_date = row['date']
                    i = index
                    date_input = input(f"Enter correct date for index {i} '{original_date}' (Format: {date_format}): ")
                    new_date = validate_date(date_input)
                    df.loc[index, 'date'] = new_date
                # Clean up the temporary validation column
                # df.drop(columns=['validated_date'], inplace=True, errors='ignore')
                # save the df in original location
                save_data(df)
            else:
                print("\n--- ✅ ALL DATES PASSED CHECK ---")
                print(f"All {len(df)} rows in the 'date' column match the format '{date_format}'.")

        # Initial check 2
        def check_null_values(file_path: str):

            try:
                df = pd.read_csv(file_path, index_col='index')
            except FileNotFoundError:
                print(f"Error: File '{file_path}' not found.")
                return

            rows_with_null = df.isnull().any(axis=1)
            empty_rows_df = df[rows_with_null]

            print("\n--- Missing Data Check ---")
            if empty_rows_df.empty:
                print("✅ No empty cells (NaN/Null/NaT) found in the DataFrame.")
            else:
                print("⚠️ The following rows contain one or more empty cells (NaN/Null/NaT):")
                print(empty_rows_df)

                # --- Instruction to the User ---
                print("\n📣 ACTION REQUIRED: Please update the missing values in your original data.")

                # Save empty rows for manual update
                empty_rows_df.to_csv('empty_rows_for_update.csv', index=False)
                print(f"These rows have been saved to 'empty_rows_for_update.csv' for review.")
                time.sleep(5)

        check_date_format(file_name, date_format)
        check_null_values(file_name)

    def load_data():
        """Loads data from the CSV file into a DataFrame."""
        if os.path.exists(config.filepath_positions):
            # Explicitly define converters for 'date' column to handle potential mixed types
            # and ensure proper parsing even if a column looks like numbers.
            # Using dayfirst=True to parse 'dd mmm yyyy' correctly.
            print(f'\n\n---- loading of File in progress-----.')
            print(f'{config.filepath_positions} loaded successfully.')
            return pd.read_csv(filepath, index_col='index')
        else:
            return print(f"{config.filepath_positions} file not found")

    def modify_existing_entry(data):
        if data.empty:
            print_android("No entries to modify.")
            return None
        print_android("\n--- Modify Entry ---")
        modify_by = input("Modify by (I)ndex or (S)tock Name? ").lower()

        selected_row_index = None
        if modify_by == 'i':
            while True:
                try:
                    idx_input = int(input("Enter the index number of the entry to modify: "))
                    if idx_input in data.index:
                        selected_row_index = idx_input
                        break
                    else:
                        print_android("Index not found. Please enter a valid index.")
                except ValueError:
                    print_android("Invalid input. Please enter an integer index.")
        elif modify_by == 's':
            stock_name_input = validate_stock_name(input("Enter the Stock Name to modify: "))
            matching_rows = data[data['stock_name'] == stock_name_input]

            if matching_rows.empty:
                print_android(f"No entry found for Stock Name: {stock_name_input}")
                return None
            elif len(matching_rows) > 1:
                print_android(f"\nMultiple entries found for {stock_name_input}:")
                # Display matching rows with formatted dates for clarity
                matching_rows_display = matching_rows.copy()
                matching_rows_display['date'] = matching_rows_display['date'].dt.strftime('%d %b %Y')
                print_android(matching_rows_display.to_string())
                while True:
                    try:
                        idx_input = int(input("Enter the index number of the specific entry to modify: "))
                        if idx_input in matching_rows.index:
                            selected_row_index = idx_input
                            break
                        else:
                            print_android("Invalid index for the selected stock name. Please try again.")
                    except ValueError:
                        print_android("Invalid input. Please enter an integer index.")
            else:
                selected_row_index = matching_rows.index[0]
        else:
            print_android("Invalid choice. Please choose 'I' or 'S'.")
            return None

        if selected_row_index is not None:
            current_row = data.loc[selected_row_index]
            print_android(f"\n--- Current Row (Index: {selected_row_index}) ---")
            # Display current date in the desired string format
            print_android(f"Date: {current_row['date']}")
            print_android(f"Stock Name: {current_row['stock_name']}")
            print_android(f"Quantity: {current_row['qty']}")
            print_android(f"Price: {current_row['price']}")
            print_android(f"Demat: {current_row['demat']}")
            print_android(f"Tran type: {current_row['tran_type']}")

            while True:
                modify_confirm = input("Confirm modification for this entry? (y/n): ").lower()
                if modify_confirm == 'y':
                    print_android("\nEnter new values (leave blank to keep current):")

                    # Display current date in input prompt in 'dd mmm yyyy' format
                    new_date_str = input(f"New Date ({current_row['date']}): ")
                    if new_date_str:
                        data.loc[selected_row_index, 'date'] = validate_date(new_date_str)

                    new_stock_name = input(f"New Stock Name ({current_row['stock_name']}): ")
                    if new_stock_name:
                        data.loc[selected_row_index, 'stock_name'] = validate_stock_name(new_stock_name)

                    new_qty_str = input(f"New Quantity ({current_row['qty']}): ")
                    if new_qty_str:
                        data.loc[selected_row_index, 'qty'] = validate_quantity(new_qty_str)

                    price_input = input(f"New Price ({current_row['price']}): ")
                    if price_input:
                        data.loc[selected_row_index, 'price'] = validate_price(price_input)

                    demat_input = input(f"New demat ({current_row['demat']}): ")
                    if demat_input:
                        data.loc[selected_row_index, 'demat'] = validate_stock_name(demat_input)  # for upper case

                    tran_input = input(f"New tran_type ({current_row['tran_type']}): ")
                    if tran_input:
                        data.loc[selected_row_index, 'tran_type'] = validate_stock_name(tran_input)  # for upper case

                    print_android("Entry updated.")
                    return data
                elif modify_confirm == 'n':
                    print_android("Modification cancelled for this entry.")
                    break
                else:
                    print_android("Invalid input. Please enter 'y' or 'n'.")

    def add_new_entry(data):
        print_android("\n--- Add New Entry ---")

        date_input = input("Enter Date (dd mmm yyyy, e.g., 01 Jan 2023): ")
        new_date = validate_date(date_input)

        stock_name_input = input("Enter Stock Name: ")
        new_stock_name = validate_stock_name(stock_name_input)

        qty_input = input("Enter Quantity: ")
        new_qty = validate_quantity(qty_input)

        price_input = input(f"Enter Price: ")
        new_price = validate_price(price_input)

        demat_input = input(f"Enter demat: ")
        new_demat = validate_stock_name(demat_input)  # for upper case

        tran_type = input(f"Enter tran_type (sell or buy): ")
        buy_or_sell = validate_stock_name(tran_type)  # for upper case

        # Determine the next index
        if data.empty:
            next_index = 0
        else:
            next_index = data.index.max() + 1

        new_entry = pd.DataFrame([{
            'date': new_date,
            'stock_name': new_stock_name,
            'qty': new_qty,
            'price': new_price,
            'demat': new_demat,
            'tran_type': buy_or_sell
        }], index=[next_index])

        data = pd.concat([data, new_entry])
        # save_data(data)
        print_android("New entry added.")
        return data

    def sort_csv(input_file="positions.csv", output_file="positions.csv"):
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
            # initial_rows = len(df)
            # df.dropna(subset=['date'], inplace=True)
            # if len(df) < initial_rows:
            #     print(f"Warning: {initial_rows - len(df)} rows were dropped due to unparseable dates.")

            # Sort the DataFrame by the 'date' column
            df_sorted = df.sort_values(by=['date', 'stock_name'], ascending=True)
    
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

    def save_data(df):
        """Sorting and saving the DataFrame to the CSV file."""
        # Create a copy to avoid modifying the original DataFrame in place
        df_to_save = df.copy()
        df_to_save.to_csv(filepath, index_label='index')
        sort_csv()
        cp = pd.read_csv(filepath)
        consolidate_positions(cp)
        consolidate_stock_positions(input_file_name=filepath)
        generate_reports(file_path=filepath)
        print_android(f"\nChanges saved to {filepath}")

    def validate_date(date_str):
        """Validates and converts a date string to 'dd mmm yyyy' format."""
        global date_format
        while True:
            try:
                dt_obj = dt.datetime.strptime(date_str, '%d %b %Y')
                # print(f'dt obj: {dt_obj} type: {type(dt_obj)}')
                formatted_date_string = dt_obj.strftime(config.date_format)
                # print(formatted_date_string)
                return formatted_date_string # Return datetime object for DataFrame
            except ValueError:
                date_str = input("Invalid date format. Please use 'dd mmm yyyy' (e.g., 01 Jan 2023): ")

    def validate_stock_name(stock_name_str):
        """Validates and converts a stock name to uppercase."""
        return stock_name_str.upper()

    def validate_quantity(qty_str):
        """Validates and converts a quantity to an integer."""
        while True:
            try:
                qty = int(qty_str)
                if qty >= 0:  # Assuming quantity should be non-negative
                    return qty
                else:
                    qty_str = input("Quantity cannot be negative. Please enter a positive integer: ")
            except ValueError:
                qty_str = input("Invalid quantity. Please enter an integer: ")

    def validate_price(price_str):
        """Validates and converts a price to 2 digit decimal."""
        while True:
            try:
                price = float(price_str)
                if price >= 0:  # Assuming price should be non-negative
                    return price
                else:
                    price_str = input("Price cannot be negative. Please enter a positive price: ")
            except ValueError:
                price_str = input("Invalid quantity. Please enter correct price: ")

    def view_records(df):
        """Displays records 10 rows at a time with an option to see more."""
        if df.empty:
            print_android("\nNo records to display.")
            return

        # Create a copy for display purposes to avoid modifying the original DataFrame
        df_display = df.copy()
        # Format the 'date' column for consistent display
        # df_display['date'] = df_display['date'].dt.strftime('%d %b %Y')

        total_rows = len(df_display)
        start_index = 0
        while start_index < total_rows:
            end_index = min(start_index + 10, total_rows)
            print_android(f"\n--- Displaying Records {start_index + 1} to {end_index} of {total_rows} ---")
            # Using .to_string() for better formatting in console output
            print(df_display.iloc[start_index:end_index].to_string())

            if end_index < total_rows:
                while True:
                    next_page = input("\nPress Enter to view next 10 records, or 'q' to return to menu: ").lower()
                    if next_page == 'q':
                        return
                    elif next_page == '':
                        break
                    else:
                        print_android("Invalid input. Press Enter or 'q'.")
            start_index += 10
        print_android("\n--- End of Records ---")

    def delete_entry(data):
        """Deletes an entry (row) from the DataFrame."""
        if data.empty:
            print_android("No entries to delete.")
            return data # Return original DataFrame if empty

        print_android("\n--- Delete Entry ---")
        delete_by = input("Delete by (I)ndex or (S)tock Name? ").lower()

        indices_to_delete = []

        if delete_by == 'i':
            while True:
                try:
                    idx_input = int(input("Enter the index number of the entry to delete: "))
                    if idx_input in data.index:
                        indices_to_delete = [idx_input]
                        break
                    else:
                        print_android("Index not found. Please enter a valid index.")
                except ValueError:
                    print_android("Invalid input. Please enter an integer index.")
        elif delete_by == 's':
            stock_name_input = validate_stock_name(input("Enter the Stock Name to delete: "))
            matching_rows = data[data['stock_name'] == stock_name_input]
            
            if matching_rows.empty:
                print_android(f"No entry found for Stock Name: {stock_name_input}")
                return data # Return original DataFrame if no match
            elif len(matching_rows) > 1:
                print_android(f"\nMultiple entries found for {stock_name_input}:")
                # Display matching rows with formatted dates for clarity
                matching_rows_display = matching_rows.copy()
                matching_rows_display['date'] = matching_rows_display['date'].dt.strftime('%d %b %Y')
                print_android(matching_rows_display.to_string())
                
                while True:
                    try:
                        idx_input = input("Enter the index number of the specific entry to delete, or 'all' to delete all matching: ")
                        if idx_input.isnumeric():
                            idx_input = int(idx_input)  # Try converting to int if not 'all'
                            if idx_input in matching_rows.index:
                                indices_to_delete = [idx_input]
                                break

                        elif idx_input == 'all':
                            indices_to_delete = matching_rows.index.tolist()
                            break
                        else:
                            print_android("Invalid index for the selected stock name. Please try again.")
                    except ValueError:
                        print_android("Invalid input. Please enter an integer index or 'all'.")
            else: # Only one matching row
                indices_to_delete = [matching_rows.index[0]]
        else:
            print_android("Invalid choice. Please choose 'I' or 'S'.")
            return data # Return original DataFrame if invalid choice

        if indices_to_delete:
            # Display entries to be deleted for confirmation
            print_android("\n--- Entry(ies) to be Deleted ---")
            entries_to_delete_display = data.loc[indices_to_delete].copy()
            # entries_to_delete_display['date'] = entries_to_delete_display['date'].dt.strftime('%d %b %Y')
            print_android(entries_to_delete_display)

            while True:
                confirm = input("Are you sure you want to delete this/these entry(ies)? (y/n): ").lower()
                if confirm == 'y':
                    data = data.drop(indices_to_delete)
                    # Reset index after deletion to maintain a continuous integer index
                    data = data.reset_index(drop=True)
                    # save_data(data)
                    print_android("Entry(ies) deleted successfully.")
                    return data # Return the modified DataFrame
                elif confirm == 'n':
                    print_android("Deletion cancelled.")
                    return data # Return original DataFrame if cancelled
                else:
                    print_android("Invalid input. Please enter 'y' or 'n'.")
        return data # Should not be reached if indices_to_delete is populated, but as a fallback

    def consolidate_stock_positions(input_file_name="positions.csv"):
        """
        Consolidates stock transaction data from a CSV file into two separate files:
        one for current holdings and one for fully sold/closed positions.

        Args:
            input_file_name (str): The name of the input CSV file.
        """
        try:
            # Load Data ---
            print(f"Reading data from {input_file_name}...")
            df = pd.read_csv(input_file_name)

            # Convert date to datetime objects for proper sorting/analysis
            # Assuming the date format is '%d %b %Y' (e.g., '12 Dec 2023')
            df['date'] = pd.to_datetime(df['date'], format='%d %b %Y', errors='coerce')

            # Convert qty and price to numeric, coercing errors to NaN
            df['qty'] = pd.to_numeric(df['qty'], errors='coerce')
            df['price'] = pd.to_numeric(df['price'], errors='coerce')

            # Ensure all stock names are strings and clean up whitespace
            df['stock_name'] = df['stock_name'].astype(str).str.strip()
            df['tran_type'] = df['tran_type'].str.upper().str.strip()

            # Calculate Transaction Value and Determine Sign ---
            # Assign a multiplier: 1 for BUY, -1 for SELL
            df['multiplier'] = df['tran_type'].apply(lambda x: 1 if x == 'BUY' else (-1 if x == 'SELL' else 0))

            # Calculate signed quantity and value for aggregation
            df['signed_qty'] = df['qty'] * df['multiplier']
            # df['transaction_value'] = df['qty'] * df['price'] * df['multiplier']

            df_buy_tran = df['signed_qty'] > 0

            print(df_buy_tran)
            return
            # Calculating buy & sell avg
            # df['avg_buy_price'] = (
            #     round(
            #         df['qty'] * df['price'] / consolidated_df['net_qty'],
            #         2)
            # ).where(consolidated_df['net_qty'] > 0)

            # Group and Aggregate ---
            # Group by stock_name and aggregate all relevant metrics
            consolidated_df = df.groupby('stock_name').agg(
                # Net Quantity: sum of signed_qty (Current Holding)
                net_qty=('signed_qty', 'sum'),
                # Total Investment/Proceeds: sum of transaction_value
                total_value=('transaction_value', 'sum'),
                # Total Quantity (absolute value of all transactions, for total trade volume)
                total_transacted_qty=('qty', 'sum'),
                # Count the number of transactions
                transaction_count=('index', 'count')
            ).reset_index()

            # --- 6. Calculate Average Price ---
            # The average buy price is the total value of the *remaining* position
            # divided by the net quantity. Only calculate for positions where net_qty > 0
            consolidated_df['avg_price'] = (
                round(
                    consolidated_df['total_value'].abs() / consolidated_df['net_qty'],
                    2)
            ).where(consolidated_df['net_qty'] > 0)
            pd.set_option('display.max_columns', None)
            print(consolidated_df)
            return
            # For display, rename columns
            consolidated_df.rename(columns={
                'net_qty': 'Current Holding Qty',
                'total_value': 'Net Value (Investment - Proceeds)',
                'avg_price': 'Avg Buy Price (For Holdings)',
                'total_transacted_qty': 'Total Transacted Qty',
                'transaction_count': 'Total Transactions'
            }, inplace=True)

            # --- 7. Classify Positions ---
            # Current Holdings: Stocks where the net quantity is greater than 0
            holdings_df = consolidated_df[consolidated_df['Current Holding Qty'] > 0].copy()

            # Sold/Closed Positions: Stocks where the net quantity is exactly 0
            sold_df = consolidated_df[consolidated_df['Current Holding Qty'] == 0].copy()

            # Clean up columns for the Sold Positions output (no need for Avg Buy Price)
            sold_df.drop(columns=['Current Holding Qty', 'Avg Buy Price (For Holdings)'], inplace=True)
            sold_df.rename(columns={
                'Net Value (Investment - Proceeds)': 'Net Profit/Loss (Proceeds - Investment)'
            }, inplace=True)

            # For sold positions, the 'Net Value' is the overall P&L,
            # which is Proceeds - Investment. Since our `transaction_value`
            # is (BUY value - SELL value), we can simply negate the result for P&L view.
            sold_df['Net Profit/Loss (Proceeds - Investment)'] = -sold_df['Net Profit/Loss (Proceeds - Investment)']

            # --- 8. Output ---
            holdings_output_file = 'current_holdings.csv'
            sold_output_file = 'sold_positions.csv'

            holdings_df.to_csv(holdings_output_file, index=False)
            sold_df.to_csv(sold_output_file, index=False)

            print(f"\nConsolidation complete!")
            print(f"Current Holdings saved to: {holdings_output_file}")
            print(f"Fully Sold/Closed Positions saved to: {sold_output_file}")

            return holdings_output_file, sold_output_file

        except FileNotFoundError:
            print(f"Error: The file '{input_file_name}' was not found.")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def consolidate_positions(df1):

        # csv file name to save
        consolidated_csv = "consolidated.csv"
        # Ensure 'date' is in 'DD MMM YYYY' format and then convert to datetime for sorting if needed
        # The original CSV has 'DD MMM YYYY' format, so we'll parse it as such
        df = df1.copy()
        # Create a temporary column for formatted date and quantity for transaction details
        # Apply lambda function row-wise using axis=1
        df['date'] = pd.to_datetime(df['date'], format='%d %b %Y', errors='coerce')
        df['date'] = df['date'].dt.strftime('%d %b %Y')
        df['formatted_date_qty'] = df.apply(lambda row: f"{row['date']} {row['qty']} x "
                                                        f"{row['price']}", axis=1)

        # Calculate total value for weighted average price
        df['total_value'] = df['qty'] * df['price']

        # Group by stock_name
        # Use named aggregation for clarity and to get desired column names
        consolidated_df = df.groupby('stock_name').agg(
            total_qty=('qty', 'sum'),
            total_value_sum=('total_value', 'sum'),  # Temporary column for weighted avg calculation
            transactions_detail=('formatted_date_qty', lambda x: '; '.join(x))
        ).reset_index()

        # Calculate weighted average price
        consolidated_df['avg_price'] = round(consolidated_df['total_value_sum'] / consolidated_df['total_qty'],2)

        # Drop the temporary total_value_sum column
        # consolidated_df = consolidated_df.drop(columns=['total_value_sum'])

        # Reorder columns to match the user's initial request order plus the new column
        # The original headers from the user were date, symbol, qty, price, demat, notes
        # For consolidated, we will have stock_name, total_qty, avg_price, transactions_detail
        consolidated_df = consolidated_df[['stock_name', 'total_qty', 'avg_price', 'total_value_sum', 'transactions_detail']]
        consolidated_df.to_csv(consolidated_csv, index_label='index')
        print_android("Consolidated csv saved successfully...")
        return

    def generate_reports(file_path):
        """
        Generates Holding and Sold reports from a stock transaction ledger (CSV).

        Args:
            file_path (str): The path to the CSV ledger file (e.g., 'positions.csv').
        """
        print("Starting report generation...")

        # --- 1. Load and Prepare Data ---
        try:
            df = pd.read_csv(file_path)
        except FileNotFoundError:
            print(f"Error: File not found at {file_path}")
            return

        # Standardizing column names for easier access (optional but good practice)
        df.columns = df.columns.str.lower().str.replace('[^a-z0-9_]', '', regex=True)

        # Data type conversion
        df['date'] = pd.to_datetime(df['date'], dayfirst=True)  # Adjust dayfirst based on your date format
        df['qty'] = pd.to_numeric(df['qty'])
        df['price'] = pd.to_numeric(df['price'])

        # Initialize a column to track the remaining quantity of each BUY transaction
        df['remaining_qty'] = np.where(df['tran_type'] == 'BUY', df['qty'], 0)

        # --- 2. Process Sales (FIFO Logic) ---

        sold_transactions = []

        # Iterate through all SELL transactions
        for _, sell_row in df[df['tran_type'] == 'SELL'].iterrows():
            stock = sell_row['stock_name']
            sell_qty = sell_row['qty']
            sell_price = sell_row['price']

            # Select all *unmatched* BUY transactions for the same stock, sorted by date (FIFO)
            buy_matches = df[
                (df['stock_name'] == stock) &
                (df['tran_type'] == 'BUY') &
                (df['remaining_qty'] > 0)
                ].sort_values(by='date')

            # Match the SELL quantity against the BUY transactions in FIFO order
            qty_to_match = sell_qty

            for buy_index, buy_row in buy_matches.iterrows():
                if qty_to_match <= 0:
                    break

                # Quantity being matched from this specific BUY lot
                qty_matched_from_lot = min(qty_to_match, buy_row['remaining_qty'])

                if qty_matched_from_lot > 0:
                    # Update the remaining quantity in the main DataFrame (in place)
                    df.loc[buy_index, 'remaining_qty'] -= qty_matched_from_lot

                    # Calculate profit for this specific portion of the sale
                    buy_cost_basis = buy_row['price']
                    profit = (sell_price - buy_cost_basis) * qty_matched_from_lot

                    # Record the matched portion for the Sold Report
                    sold_transactions.append({
                        'stock_name': stock,
                        'qty_sold': qty_matched_from_lot,
                        'buy_price': buy_cost_basis,
                        'sell_price': sell_price,
                        'profit': profit
                    })

                    qty_to_match -= qty_matched_from_lot

        # --- 3. Create Sold Report ---

        if sold_transactions:
            sold_df = pd.DataFrame(sold_transactions)

            # Group by stock name to get the final summarized report
            sold_report = sold_df.groupby('stock_name').agg(
                qty_sold=('qty_sold', 'sum'),
                total_buy_value=('buy_price', lambda x: (x * sold_df.loc[x.index, 'qty_sold']).sum()),
                total_sell_value=('sell_price', lambda x: (x * sold_df.loc[x.index, 'qty_sold']).sum()),
                profit=('profit', 'sum')
            ).reset_index()

            # Calculate the final average buy and sell prices
            sold_report['avg_buy_price'] = sold_report['total_buy_value'] / sold_report['qty_sold']
            sold_report['avg_sell_price'] = sold_report['total_sell_value'] / sold_report['qty_sold']

            # Select and format final columns
            sold_report_final = sold_report[[
                'stock_name', 'avg_buy_price', 'avg_sell_price', 'qty_sold', 'profit'
            ]]

            # Save the Sold Report
            sold_report_final.to_csv('sold_report.csv', index=False, float_format='%.2f')
            print("✅ Sold Report saved to 'sold_report.csv'")
        else:
            print("ℹ️ No SELL transactions found or processed.")

        # --- 4. Create Holding Report ---

        # Filter for remaining BUY lots (where remaining_qty > 0)
        holding_lots = df[
            (df['tran_type'] == 'BUY') &
            (df['remaining_qty'] > 0)
            ].copy()

        # Calculate the total value of the remaining holding for weighted average
        holding_lots['total_cost'] = holding_lots['remaining_qty'] * holding_lots['price']

        # Group by stock name
        holding_report = holding_lots.groupby('stock_name').agg(
            qty_holding=('remaining_qty', 'sum'),
            total_cost=('total_cost', 'sum')
        ).reset_index()

        # Calculate the average buy price (Total Cost / Total Qty)
        holding_report['avg_buy_price'] = holding_report['total_cost'] / holding_report['qty_holding']

        # Select and format final columns
        holding_report_final = holding_report[[
            'stock_name', 'qty_holding', 'avg_buy_price'
        ]]

        # Save the Holding Report
        holding_report_final.to_csv('holding_report.csv', index=False, float_format='%.2f')
        print("✅ Holding Report saved to 'holding_report.csv'")

    # checking data initially
    initial_check()

    # Load data initially
    df = load_data()

    while True:
        print_android("\n--- CSV Operation Menu ---")
        print_android("1. Modify existing entry")
        print_android("2. Add new entry")
        print_android("3. View all records")
        print_android("4. Delete entry") # New option
        print_android("5. Exit") # Shifted
        print_android("6. generate reports")
        print_android("0. Clear console")  # Shifted

        choice = input("Enter your choice (1-6): ")

        if choice == '1':
            df = modify_existing_entry(data=df)
            save_data(df)
        elif choice == '2':
            df = add_new_entry(data=df)
            save_data(df)
        elif choice == '3':
            view_records(df)
        elif choice == '4': # New delete functionality
            df = delete_entry(data=df) # Update the DataFrame with the result of deletion
            save_data(df)

        elif choice == '5': # Shifted option
            clear_console()
            break

        elif choice == '6': # Shifted option
            save_data(df)

        elif choice == '0': # Shifted option
            save_data(df)
            time.sleep(1)  # Pause for 3 seconds so you can see the text before it clears
            print_android("Clearing Console.")
            clear_console()
        else:
            print_android("Invalid choice. Please enter a number between 1 and 6.")

# Example usage:
if __name__ == "__main__":
    # Create a dummy positions.csv for demonstration if it doesn't exist
    if not os.path.exists('positions.csv'):
        initial_data = {
            'index': [i for i in range(15)], # More than 10 entries
            'date': [f'{i+1:02d} Jan 2023' for i in range(15)],
            'stock_name': [f'STOCK{chr(65 + i % 5)}' for i in range(15)],
            'qty': [10 + i for i in range(15)]
        }
        initial_df = pd.DataFrame(initial_data).set_index('index')
        # Convert to datetime objects before saving to ensure 'date' column is consistent
        initial_df['date'] = pd.to_datetime(initial_df['date'], format='%d %b %Y') 
        initial_df.to_csv('positions.csv', index_label='index', date_format='%d %b %Y')
        print_android("Created a dummy 'positions.csv' with more than 10 entries for demonstration.")


    file_operate('positions.csv')

