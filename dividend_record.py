import csv
from datetime import datetime
from collections import defaultdict

# The name of our data file
CSV_FILE = 'dividends.csv'
# The desired date format for input and output
DATE_FORMAT = '%d %b %Y'


def read_dividends_from_csv():
    """Reads dividend data from the CSV file.
    Returns a list of lists, where each inner list is a row from the CSV.
    Handles the case where the file doesn't exist."""
    try:
        with open(CSV_FILE, 'r', newline='') as f:
            reader = csv.reader(f)
            # Check for an empty file
            try:
                header = next(reader)
            except StopIteration:
                return []
            return [row for row in reader]
    except FileNotFoundError:
        return []


def write_dividends_to_csv(dividends_data):
    """Writes the dividend data to the CSV file after sorting it by date.
    This function overwrites the entire file with the new data."""
    try:
        sorted_data = sorted(dividends_data, key=lambda x: datetime.strptime(x[0], DATE_FORMAT))
    except (ValueError, IndexError):
        # Fallback to unsorted data if date format is inconsistent
        print("Warning: Date format issue detected, saving data without sorting.")
        sorted_data = dividends_data

    with open(CSV_FILE, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['Date', 'Stock', 'Amount'])
        writer.writerows(sorted_data)


def display_records(records):
    """Helper function to display all records with an index for user selection.
    Returns True if records exist, False otherwise."""
    if not records:
        print("No records found.")
        return False
    print("\n--- Current Dividend Records ---")
    # Display records with a user-friendly index starting from 1
    for i, row in enumerate(records):
        print(f"[{i + 1}] Date: {row[0]}, Stock: {row[1]}, Amount: ${float(row[2]):,.2f}")
    print("------------------------------")
    return True


def validate_input(date_str, stock_ticker, amount_str):
    """Validates user input based on the specified formats."""
    try:
        if date_str:
            datetime.strptime(date_str, DATE_FORMAT)
    except ValueError:
        return False, f"Invalid date format. Please use {DATE_FORMAT}."

    if not stock_ticker:
        return False, "Stock ticker cannot be empty."

    try:
        if amount_str:
            amount = float(amount_str)
            if amount <= 0:
                return False, "Amount must be a positive number."
    except ValueError:
        return False, "Invalid amount. Please enter a number."

    return True, None


def get_validated_dividend_input(old_data=None):
    """Prompts the user for dividend information and continuously validates it.
    If old_data is provided, it shows the current values and allows skipping.
    Returns a tuple of validated data: (date_str, stock_ticker, amount_str)."""
    old_date = old_data[0] if old_data else ""
    old_stock = old_data[1] if old_data else ""
    old_amount = str(old_data[2]) if old_data else ""

    while True:
        date_str = input(f"Enter date ({DATE_FORMAT}) [{old_date}]: ") or old_date
        stock_ticker = input(f"Enter stock ticker [{old_stock}]: ").upper() or old_stock
        amount_str = input(f"Enter amount received [{old_amount}]: ") or old_amount

        is_valid, error_msg = validate_input(date_str, stock_ticker, amount_str)

        if is_valid:
            # We must convert the amount to a float before returning
            return date_str, stock_ticker, float(amount_str)
        else:
            print(f"Error: {error_msg}")
            print("Please try again.")


def record_dividend():
    """Allows the user to record a new dividend entry."""
    print("\n--- Record a new dividend ---")
    date_str, stock_ticker, amount = get_validated_dividend_input()
    dividends = read_dividends_from_csv()
    dividends.append([date_str, stock_ticker, amount])
    write_dividends_to_csv(dividends)
    print("Dividend recorded successfully!")


def view_records_paginated():
    """Displays all records in batches of 10."""
    dividends = read_dividends_from_csv()
    if not dividends:
        print("No records found to display.")
        return

    page_size = 10
    total_records = len(dividends)
    for i in range(0, total_records, page_size):
        print(f"\n--- Records {i + 1} to {min(i + page_size, total_records)} of {total_records} ---")
        for j in range(i, min(i + page_size, total_records)):
            row = dividends[j]
            print(f"[{j + 1}] Date: {row[0]}, Stock: {row[1]}, Amount: ${float(row[2]):,.2f}")

        if i + page_size < total_records:
            input("\nPress Enter to view the next 10 records, or 'q' to return to the menu.")
            # This second input() call was a bug, removed to fix the pagination logic
            # now we just wait for the user to press Enter.


def modify_dividend():
    """Allows the user to modify an existing dividend entry by index."""
    dividends = read_dividends_from_csv()
    if not display_records(dividends):
        return

    try:
        # We subtract 1 to get the correct list index since we display a 1-based index
        index_to_modify = int(input("Enter the index of the record to modify: ")) - 1
        if not (0 <= index_to_modify < len(dividends)):
            print("Invalid index. Please try again.")
            return
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    old_record = dividends[index_to_modify]

    print("\nEnter the new values. Press Enter to keep the current value.")
    date_str, stock_ticker, amount = get_validated_dividend_input(old_data=old_record)

    dividends[index_to_modify] = [date_str, stock_ticker, amount]
    write_dividends_to_csv(dividends)
    print("Record modified successfully!")


def delete_dividend():
    """Allows the user to delete an existing dividend entry by index."""
    dividends = read_dividends_from_csv()
    if not display_records(dividends):
        return

    try:
        # We subtract 1 to get the correct list index since we display a 1-based index
        index_to_delete = int(input("Enter the index of the record to delete: ")) - 1
        if not (0 <= index_to_delete < len(dividends)):
            print("Invalid index. Please try again.")
            return
    except ValueError:
        print("Invalid input. Please enter a number.")
        return

    del dividends[index_to_delete]
    write_dividends_to_csv(dividends)
    print("Record deleted successfully!")


def generate_report():
    """Generates a summary report of dividends by reading the CSV data."""
    dividends = read_dividends_from_csv()
    if not dividends:
        print("No dividend records found to generate a report.")
        return

    stock_summary = defaultdict(float)
    total_dividend = 0.0

    for row in dividends:
        try:
            stock_summary[row[1]] += float(row[2])
            total_dividend += float(row[2])
        except (ValueError, IndexError) as e:
            print(f"Skipping malformed row: {row}. Error: {e}")

    if not stock_summary:
        print("No valid dividend records found.")
        return

    print("\n--- Dividend Report ---")
    print("-----------------------")
    for stock, amount in sorted(stock_summary.items()):
        print(f"Stock: {stock:<10} | Total Dividend: ${amount:,.2f}")
    print("-----------------------")
    print(f"Total Dividend Received: ${total_dividend:,.2f}")


def main_menu():
    """The main menu loop that drives the application."""
    while True:
        print("\n--- Main Menu ---")
        print("1. Record a new dividend")
        print("2. Generate a dividend report")
        print("3. View all records")
        print("4. Modify a dividend")
        print("5. Delete a dividend")
        print("6. Exit")

        choice = input("Enter your choice: ")

        if choice == '1':
            record_dividend()
        elif choice == '2':
            generate_report()
        elif choice == '3':
            view_records_paginated()
        elif choice == '4':
            modify_dividend()
        elif choice == '5':
            delete_dividend()
        elif choice == '6':
            print("Exiting program. Goodbye!")
            break
        else:
            print("Invalid choice. Please enter a valid number.")


if __name__ == "__main__":
    main_menu()