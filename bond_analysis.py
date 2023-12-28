from datetime import datetime
import pandas as pd


def filter_bonds_by_coupon_percentile(df):
    # Convert 'Cpn' column to numeric
    df['Cpn'] = pd.to_numeric(df['Cpn'], errors='coerce')
    df = df.dropna(subset=['Cpn'])

    avg_coupon_df = df.groupby('Ticker')['Cpn'].mean().reset_index()
    percentile_df = df.groupby('Ticker')['Cpn'].quantile(0.70).reset_index()

    filtered_df = pd.merge(df, percentile_df, on='Ticker', suffixes=('', '_percentile'))
    filtered_df = filtered_df[filtered_df['Cpn'] <= filtered_df['Cpn_percentile']]

    return filtered_df


def main():
    # Load the original bonds data
    file_path = "C:\\Users\\Newia\\bonds.xlsx"
    cds_file_path = "C:\\Users\\Newia\\cds.xlsx"
    # Read and preview the cds data
    cds_df = pd.read_excel(cds_file_path, engine='openpyxl')
    print(f"Number of rows in cds data: {len(cds_df)}")
    df = pd.read_excel(file_path, engine='openpyxl')

    print(cds_df.columns)

    print(f"Number of rows before filtering: {len(df)}")

    # Filter bonds based on coupon percentile
    filtered_bonds_df = filter_bonds_by_coupon_percentile(df)

    print(f"Number of rows after filtering by coupon percentile: {len(filtered_bonds_df)}")

    # Define unstable countries
    unstable_countries = ["ISRAEL", "RUSSIA", "UKRAIN"]

    # Filter out bonds fromunstable countries
    filtered_bonds_df = filtered_bonds_df[~filtered_bonds_df['Ticker'].isin(unstable_countries)]

    print(f"Number of rows after filtering unstable countries: {len(filtered_bonds_df)}")

    # Convert Maturity column to datetime format
    filtered_bonds_df['Maturity'] = pd.to_datetime(filtered_bonds_df['Maturity'], errors='coerce')

    # Convert the string '2024-11-24' to a datetime object
    cutoff_date = datetime.strptime('2024-11-24', '%Y-%m-%d')

    # Filter out rows where 'Maturity' date is before 24th Novöember 2024
    filtered_bonds_df = filtered_bonds_df.loc[filtered_bonds_df['Maturity'] >= cutoff_date]

    print(f"Number of rows after filtering by maturity date: {len(filtered_bonds_df)}")

    # Remove rows where all columns have the same values as another row (keeping only the first occurrence)
    filtered_bonds_df = filtered_bonds_df.drop_duplicates()
    print(f"Number of rows after removing duplicates: {len(filtered_bonds_df)}")

    # Removing callables and sinkables
    filtered_bonds_df = filtered_bonds_df[~filtered_bonds_df['Mty Type'].isin(['CALL/SINK', 'CALLABLE', 'SINKABLE'])]
    print(f"Number of rows after filtering out 'CALL/SINK' and 'CALLABLE' from 'Mty Type': {len(filtered_bonds_df)}")

    # Eliminate 10% of the rows with the lowest "Amt Out" and keep the top 90%
    amt_out_threshold = filtered_bonds_df['Amt Out'].quantile(0.1)
    filtered_bonds_df = filtered_bonds_df[filtered_bonds_df['Amt Out'] >= amt_out_threshold]
    print(f"Number of rows after filtering by amt out: {len(filtered_bonds_df)}")

    # Ensure numeric values for 'Yld to Mty (Ask)' and 'Yld to Mty (Bid)' and filter based on YTM Bid-to-Ask ratio
    filtered_bonds_df[['Yld to Mty (Bid)', 'Yld to Mty (Ask)']] = filtered_bonds_df[
        ['Yld to Mty (Bid)', 'Yld to Mty (Ask)']].apply(pd.to_numeric, errors='coerce')
    filtered_bonds_df = filtered_bonds_df[
        (filtered_bonds_df['Yld to Mty (Bid)'] / filtered_bonds_df['Yld to Mty (Ask)']).lt(1.0209)]
    print(f"Number of rows after filtering by YTM Bid-to-Ask ratio and low spread: {len(filtered_bonds_df)}")

    # Remove columns "BVAL Ask Yld" , "BVAL Bid Yld" , "BBG Composite" , "Seriea" "
    filtered_bonds_df = filtered_bonds_df.drop(columns=["BVAL Ask Yld", "BVAL Bid Yld", "BBG Composite", "Series"])

    # Print the columns of the DataFrame after removing the specified columns
    print(f"Columns after removing 'BVAL Ask Yld' and 'BVAL Bid Yld':")
    print(filtered_bonds_df.columns)

    # Save the filtered data to a new Excel file
    save_path = "C:\\Users\\Gizem Öztok Altınsaç\\Downloads\\Things\\filtered_bonds.xlsx"
    filtered_bonds_df.to_excel(save_path, index=False)
    print(f"Filtered data saved to {save_path}")

    def compute_weighted_average(cds_df):
        # Print column names to check for the exact name of the 'Spread 5Y ' column
        print(cds_df.columns)

        if 'Spread 5Y ' in cds_df.columns:
            cds_df['Weighted_Spread_5y'] = cds_df['Spread 5Y '] * 0.30
        else:
            print("Column 'Spread 5Y ' not found in the DataFrame.")
            return None

        if 'Spread 2Y' in cds_df.columns:
            cds_df['Weighted_Spread_2y'] = cds_df['Spread 2Y'] * 0.70
        else:
            print("Column 'Spread 2Y' not found in the DataFrame.")
            return None

        weighted_avg_df = cds_df.groupby('Name').agg({
            'Weighted_Spread_2y': 'sum',
            'Weighted_Spread_5y': 'sum'
        }).reset_index()

        weighted_avg_df['Final_Weighted_Avg_Spread'] = weighted_avg_df['Weighted_Spread_2y'] + weighted_avg_df[
            'Weighted_Spread_5y']
        return weighted_avg_df[['Name', 'Final_Weighted_Avg_Spread']]

    # call the compute_weighted_average function
    cds_file_path = "C:\\Users\\Newia\\cds.xlsx"
    cds_df = pd.read_excel(cds_file_path, engine='openpyxl')

    # Compute and save the weighted averages
    weighted_avg_data = compute_weighted_average(cds_df)
    print(weighted_avg_data.head())  # Print the first few rows of the computed data

    # save the weighted_avg_data
    weighted_avg_save_path = "C:\\Users\\Gizem Öztok Altınsaç\\Downloads\\Things\\weighted_avg_data.xlsx"

   # Excel file
    weighted_avg_data.to_excel(weighted_avg_save_path, index=False)
    print(f"Weighted average data saved to {weighted_avg_save_path}")

# File path
file_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\filtered_bonds.xlsx"

# Excel file into a pandas DataFrame
df = pd.read_excel(file_path)

# List of known country names and entities for extraction
countries = [
    "Abu Dhabi", "Argentina", "Australia", "Austria", "Bahrain", "Belgium", "Brazil", "Bulgaria",
    "Canada", "Chile", "China", "Colombia", "Costa Rica", "Croatia", "Cyprus", "Czech",
    "Denmark", "Dominican Republic", "Dubai", "Ecuador", "Egypt", "El Salvador", "Estonia",
    "Finland", "France", "Gabon", "Germany", "Greece", "Guatemala", "Hong Kong", "Hungary",
    "Iceland", "India", "Indonesia", "Iraq", "Ireland", "Israel", "Italy", "Japan", "Kazakhstan",
    "Korea", "Kuwait", "Latvia", "Lithuania", "Malaysia", "Mexico", "Morocco", "Netherlands",
    "New Zealand", "Nigeria", "Norway", "Oman", "Pakistan", "Panama", "Peru", "Philippines",
    "Poland", "Portugal", "Qatar", "Romania", "Saudi Arabia", "Serbia", "Singapore", "Slovakia",
    "Slovenia", "South Africa", "Spain", "Sweden", "Switzerland", "Thailand", "Tunisia", "Turkey",
    "United Kingdom", "United States", "Uruguay", "Venezuela", "Vietnam", "Philippine"
]
# Function to extract country name
def extract_country(name):
    for country in countries:
        if country.lower() in name.lower():
            return country
    return 'Unknown'

# extract country names from the 'Name' column and store in a new 'Country' column
df['Country'] = df['Name'].apply(extract_country)

# Save the DataFrame to a new Excel file
output_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\filtered_bonds_with_country_names.xlsx"
df.to_excel(output_path, index=False)

# Display the first few rows of the DataFrame
print(df[['Name', 'Country']].head())

if __name__ == "__main__":
    # Step 1: Read the data
    filtered_bonds_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\filtered_bonds_with_country_names.xlsx"
    weighted_avg_data_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\weighted_avg_data.xlsx"

    filtered_bonds_df = pd.read_excel(filtered_bonds_path)
    weighted_avg_df = pd.read_excel(weighted_avg_data_path)

    # Step 2: Merge on 'Country'
    merged_df = pd.merge(filtered_bonds_df, weighted_avg_df, left_on='Country', right_on='Name', how='left')

    # Update 'Country' based on specific conditions
    merged_df.loc[merged_df['Name_x'].str.contains('Federal', case=False, na=False), 'Country'] = 'United States'
    merged_df.loc[merged_df['Name_x'].str.contains('Korea', case=False, na=False), 'Country'] = 'South Korea'
    merged_df.loc[merged_df['Name_x'].str.contains('Turkiye', case=False, na=False), 'Country'] = 'Turkey'
    merged_df.loc[merged_df['Name_x'].str.contains('Deutschland', case=False, na=False), 'Country'] = 'Germany'
    merged_df.loc[merged_df['Name_x'].str.contains('French', case=False, na=False), 'Country'] = 'France'
    merged_df.loc[merged_df['Name_x'].str.contains('Hellenic', case=False, na=False), 'Country'] = 'Greece'
    merged_df.loc[merged_df['Name_x'].str.contains('Bundes', case=False, na=False), 'Country'] = 'Germany'
    merged_df.loc[merged_df['Name_x'].str.contains('Romanian', case=False, na=False), 'Country'] = 'Romania'
    merged_df.loc[merged_df['Name_x'].str.contains('Philippine', case=False, na=False), 'Country'] = 'Philippines'

    # Step 3: Map the 'Final_Weighted_Avg_Spread' values based on the 'Name' column from weighted_avg_data
    country_cds_mapping = weighted_avg_df.set_index('Name')['Final_Weighted_Avg_Spread'].to_dict()

    # Update the 'Final_Weighted_Avg_Spread' in merged_df based on the country_cds_mapping
    merged_df['Final_Weighted_Avg_Spread'] = merged_df['Country'].map(country_cds_mapping)

    # Step 4: Group by 'Country' and compute the mean of 'Final_Weighted_Avg_Spread'
    grouped = merged_df.groupby('Country')['Final_Weighted_Avg_Spread'].mean().reset_index()

    # Step 5: Merge the grouped data back to ensure consistent values for 'Final_Weighted_Avg_Spread'
    merged_df = pd.merge(merged_df.drop(columns=['Final_Weighted_Avg_Spread']),
                         grouped,
                         on='Country',
                         how='left',
                         suffixes=('', '_new'))

    # Replace NaN values in 'Final_Weighted_Avg_Spread_new' with the original values to keep them intact
    merged_df['Final_Weighted_Avg_Spread'].fillna(merged_df['Final_Weighted_Avg_Spread'], inplace=True)

    # Rename the updated column to 'Final_Weighted_Avg_Spread'
    merged_df.rename(columns={'Final_Weighted_Avg_Spread_new': 'Final_Weighted_Avg_Spread'}, inplace=True)

    # Display updated merged data
    print(merged_df.head())

    # Drop rows where 'Final_Weighted_Avg_Spread' is NaN
    merged_df = merged_df.dropna(subset=['Final_Weighted_Avg_Spread'])

    # Display updated merged data
    print(merged_df.head())

    # Convert 'Final_Weighted_Avg_Spread' from cds to percentage
    # Given that 100 cds = 1%, we can simply divide by 100
    merged_df['Final_Weighted_Avg_Spread_Percentage'] = merged_df['Final_Weighted_Avg_Spread'] / 100

    merged_df['Yld to Mty (Bid)_Numerical'] = merged_df['Yld to Mty (Bid)']

    # Subtract the percentage value of 'Final_Weighted_Avg_Spread' from the value of 'Yld to Mty (Bid)'
    merged_df['Y-CDS'] = merged_df['Yld to Mty (Bid)_Numerical'] - merged_df['Final_Weighted_Avg_Spread_Percentage']


    # merged_df['Y-CDS'] = merged_df['Y-CDS'] * 100

    # Display the updated DataFrame
    print(merged_df.head())

    # Sort the DataFrame in descending order based on the 'Y-CDS' column
    merged_df = merged_df.sort_values(by='Y-CDS', ascending=False)

    # Display the sorted DataFrame
    print(merged_df.head())

    # Step 6: Save merged data to a new Excel file
    merged_output_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\merged_bonds_cds_data.xlsx"
    merged_df.to_excel(merged_output_path, index=False)
    print(f"Merged data saved to {merged_output_path}")

# Define the base path
base_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\Analysis Final"

# Filter rows where "Final_Weighted_Avg_Spread_Percentage" is less than 1
less_than_1_df = merged_df[merged_df['Final_Weighted_Avg_Spread_Percentage'] < 1]

# Save to Excel
less_than_1_path = f"{base_path}\\less_than_1.xlsx"
less_than_1_df.to_excel(less_than_1_path, index=False)
print(f"Data with 'Final_Weighted_Avg_Spread_Percentage' less than 1 saved to {less_than_1_path}")

# Filter rows where "Final_Weighted_Avg_Spread_Percentage" is less than 3
less_than_3_df = merged_df[merged_df['Final_Weighted_Avg_Spread_Percentage'] < 3]

# Save to Excel
less_than_3_path = f"{base_path}\\less_than_3.xlsx"
less_than_3_df.to_excel(less_than_3_path, index=False)
print(f"Data with 'Final_Weighted_Avg_Spread_Percentage' less than 3 saved to {less_than_3_path}")

# Filter rows where "Y-CDS" is less than 5 for both less_than_1_df and less_than_3_df
less_than_1_filtered_df = less_than_1_df[less_than_1_df['Y-CDS'] >= 5]
less_than_3_filtered_df = less_than_3_df[less_than_3_df['Y-CDS'] >= 5]

# Define the base path
base_path = r"C:\Users\Gizem Öztok Altınsaç\Downloads\Things\Analysis Final"

# Save the filtered datasets to Excel
less_than_1_filtered_path = f"{base_path}\\less_than_1_filtered.xlsx"
less_than_1_filtered_df.to_excel(less_than_1_filtered_path, index=False)
print(f"Filtered data with 'Final_Weighted_Avg_Spread_Percentage' less than 1 and 'Y-CDS' >= 5 saved to {less_than_1_filtered_path}")

less_than_3_filtered_path = f"{base_path}\\less_than_3_filtered.xlsx"
less_than_3_filtered_df.to_excel(less_than_3_filtered_path, index=False)
print(f"Filtered data with 'Final_Weighted_Avg_Spread_Percentage' less than 3 and 'Y-CDS' >= 5 saved to {less_than_3_filtered_path}")


if __name__ == "__main__":
    main()
