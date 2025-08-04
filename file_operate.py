import os
import pandas as pd
import datetime as dt
import time

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

    def load_data():
        """Loads data from the CSV file into a DataFrame."""
        if os.path.exists(filepath):
            # Explicitly define converters for 'date' column to handle potential mixed types
            # and ensure proper parsing even if a column looks like numbers.
            # Using dayfirst=True to parse 'dd mmm yyyy' correctly.
            return pd.read_csv(filepath, index_col='index', parse_dates=['date'], dayfirst=True)
        return pd.DataFrame(columns=['date', 'stock_name', 'qty'])

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

    def save_data(df):
        """Saves the DataFrame to the CSV file."""
        # Create a copy to avoid modifying the original DataFrame in place
        df_to_save = df.copy()
        # Ensure 'date' column is in 'dd mmm yyyy' format before saving
        df_to_save['date'] = df_to_save['date'].dt.strftime('%d %b %Y')
        
        # df_to_save = df_to_save.sort_values(by='date', ascending=False) 
        df_to_save.to_csv(filepath, index_label='index')
        sort_csv()
        cp = pd.read_csv(filepath)
        consolidate_positions(cp)
        print_android(f"\nChanges saved to {filepath}")

    def validate_date(date_str):
        """Validates and converts a date string to 'dd mmm yyyy' format."""
        while True:
            try:
                dt_obj = dt.datetime.strptime(date_str, '%d %b %Y')
                return dt_obj # Return datetime object for DataFrame
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
        df_display['date'] = df_display['date'].dt.strftime('%d %b %Y')

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

    def delete_entry(df):
        """Deletes an entry (row) from the DataFrame."""
        if df.empty:
            print_android("No entries to delete.")
            return df # Return original DataFrame if empty

        print_android("\n--- Delete Entry ---")
        delete_by = input("Delete by (I)ndex or (S)tock Name? ").lower()

        indices_to_delete = []

        if delete_by == 'i':
            while True:
                try:
                    idx_input = int(input("Enter the index number of the entry to delete: "))
                    if idx_input in df.index:
                        indices_to_delete = [idx_input]
                        break
                    else:
                        print_android("Index not found. Please enter a valid index.")
                except ValueError:
                    print_android("Invalid input. Please enter an integer index.")
        elif delete_by == 's':
            stock_name_input = validate_stock_name(input("Enter the Stock Name to delete: "))
            matching_rows = df[df['stock_name'] == stock_name_input]
            
            if matching_rows.empty:
                print_android(f"No entry found for Stock Name: {stock_name_input}")
                return df # Return original DataFrame if no match
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
            return df # Return original DataFrame if invalid choice

        if indices_to_delete:
            # Display entries to be deleted for confirmation
            print_android("\n--- Entry(ies) to be Deleted ---")
            entries_to_delete_display = df.loc[indices_to_delete].copy()
            entries_to_delete_display['date'] = entries_to_delete_display['date'].dt.strftime('%d %b %Y')
            print_android(entries_to_delete_display.to_string())

            while True:
                confirm = input("Are you sure you want to delete this/these entry(ies)? (y/n): ").lower()
                if confirm == 'y':
                    df = df.drop(indices_to_delete)
                    # Reset index after deletion to maintain a continuous integer index
                    df = df.reset_index(drop=True)
                    print_android("Entry(ies) deleted successfully.")
                    return df # Return the modified DataFrame
                elif confirm == 'n':
                    print_android("Deletion cancelled.")
                    return df # Return original DataFrame if cancelled
                else:
                    print_android("Invalid input. Please enter 'y' or 'n'.")
        return df # Should not be reached if indices_to_delete is populated, but as a fallback

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

    # Load data initially
    df = load_data()

    while True:
        print_android("\n--- CSV Operation Menu ---")
        print_android("1. Modify existing entry")
        print_android("2. Add new entry")
        print_android("3. View all records")
        print_android("4. Delete entry") # New option
        print_android("5. Save and Exit") # Shifted
        print_android("6. Exit without saving") # Shifted
        print_android("0. Clear console")  # Shifted

        choice = input("Enter your choice (1-6): ")

        if choice == '1':
            if df.empty:
                print_android("No entries to modify.")
                continue

            print_android("\n--- Modify Entry ---")
            modify_by = input("Modify by (I)ndex or (S)tock Name? ").lower()

            selected_row_index = None
            if modify_by == 'i':
                while True:
                    try:
                        idx_input = int(input("Enter the index number of the entry to modify: "))
                        if idx_input in df.index:
                            selected_row_index = idx_input
                            break
                        else:
                            print_android("Index not found. Please enter a valid index.")
                    except ValueError:
                        print_android("Invalid input. Please enter an integer index.")
            elif modify_by == 's':
                stock_name_input = validate_stock_name(input("Enter the Stock Name to modify: "))
                matching_rows = df[df['stock_name'] == stock_name_input]
                
                if matching_rows.empty:
                    print_android(f"No entry found for Stock Name: {stock_name_input}")
                    continue
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
                continue

            if selected_row_index is not None:
                current_row = df.loc[selected_row_index]
                print_android(f"\n--- Current Row (Index: {selected_row_index}) ---")
                # Display current date in the desired string format
                print_android(f"Date: {current_row['date'].strftime('%d %b %Y')}")
                print_android(f"Stock Name: {current_row['stock_name']}")
                print_android(f"Quantity: {current_row['qty']}")
                print_android(f"Price: {current_row['price']}")
                print_android(f"Demat: {current_row['demat']}")

                while True:
                    modify_confirm = input("Confirm modification for this entry? (y/n): ").lower()
                    if modify_confirm == 'y':
                        print_android("\nEnter new values (leave blank to keep current):")
                        
                        # Display current date in input prompt in 'dd mmm yyyy' format
                        new_date_str = input(f"New Date ({current_row['date'].strftime('%d %b %Y')}): ")
                        if new_date_str:
                            df.loc[selected_row_index, 'date'] = validate_date(new_date_str)
                        
                        new_stock_name = input(f"New Stock Name ({current_row['stock_name']}): ")
                        if new_stock_name:
                            df.loc[selected_row_index, 'stock_name'] = validate_stock_name(new_stock_name)
                        
                        new_qty_str = input(f"New Quantity ({current_row['qty']}): ")
                        if new_qty_str:
                            df.loc[selected_row_index, 'qty'] = validate_quantity(new_qty_str)

                        price_input = input(f"New Price ({current_row['price']}): ")
                        if price_input:
                            df.loc[selected_row_index, 'price'] = validate_price(price_input)

                        demat_input = input(f"New demat ({current_row['demat']}): ")
                        if demat_input:
                            df.loc[selected_row_index, 'demat'] = validate_stock_name(demat_input) # for upper case
                        
                        print_android("Entry updated.")
                        break
                    elif modify_confirm == 'n':
                        print_android("Modification cancelled for this entry.")
                        break
                    else:
                        print_android("Invalid input. Please enter 'y' or 'n'.")

        elif choice == '2':
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

            # Determine the next index
            if df.empty:
                next_index = 0
            else:
                next_index = df.index.max() + 1
            
            new_entry = pd.DataFrame([{
                'date': new_date,
                'stock_name': new_stock_name, 
                'qty': new_qty,
                'price': new_price,
                'demat': new_demat
            }], index=[next_index])
            
            df = pd.concat([df, new_entry])
            print_android("New entry added.")

        elif choice == '3':
            view_records(df)
            
        elif choice == '4': # New delete functionality
            df = delete_entry(df) # Update the DataFrame with the result of deletion

        elif choice == '5': # Shifted option
            save_data(df)

            clear_console()

            break

        elif choice == '6': # Shifted option
            print_android("Exiting without saving changes....")
            print_android("Exiting program...")
            time.sleep(2)
            break




        elif choice == '0': # Shifted option
            print_android("Clearing Console.")
            time.sleep(1)  # Pause for 3 seconds so you can see the text before it clears
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
