import pandas as pd


def remove_dups(input_df, output_df):
    """To remove duplicates from csv
    args:
        input_df = path of input csv
        output_df = path of output csv
     """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_df)  # Replace "your_file.csv" with your actual file name
    # df = input_df
    
    # Drop duplicate rows based on all columns
    df = df.drop_duplicates()
    print("Duplicates removed") 
    
    # Write the cleaned data to a new CSV file
    df.to_csv(output_df, index=False)  # Replace "cleaned_data.csv" with your desired output file name
    print(f"file saved at {output_df} ")
   
input_df = pd.read_csv("logs/trade_log.csv")
# input_df = input_df.drop(columns=['Profit'])
print(input_df)
# remove_dups(input_df=input_df, output_df="logs/trade_log.csv")
# print(input_df)