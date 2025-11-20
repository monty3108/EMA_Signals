import pandas as pd
import numpy as np


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


# --- Execution ---
# Replace 'positions.csv' with the actual path if your file is named differently
generate_reports('positions.csv')