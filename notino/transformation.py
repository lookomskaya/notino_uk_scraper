import pandas as pd

class NotinoTransformation:
    def __init__(self, country: str, retailer: str):
        self.country = country
        self.retailer = retailer

    def transform_data(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        transformed_df = raw_df.copy()
        
        # Transforming 'Scraping Timestamp' column
        transformed_df['Scraping Timestamp'] = transformed_df['Scraping Timestamp'].astype('datetime64')
        transformed_df = transformed_df.rename(columns={'Scraping Timestamp': 'Scraped_at'})
        
        # Adding 'Currency' column
        transformed_df['Currency'] = '£'
        
        # Adding 'Country' column
        transformed_df['Country'] = self.country
        
        # Transforming 'Discount' column
        transformed_df['Discount'] = transformed_df['Discount'].fillna(0)
        transformed_df['Discount'] = transformed_df['Discount'].str.replace('%', '').astype('float64')
        transformed_df['Discount'] = transformed_df['Discount'] / 100 * (-1)
        
        # Calculating 'Discount amount' and 'Price after sale'
        transformed_df['Discount amount'] = transformed_df['Price'] * transformed_df['Discount']
        transformed_df['Discount amount'] = transformed_df['Discount amount'].fillna(0)
        transformed_df['Price after sale'] = transformed_df['Price'] - transformed_df['Discount amount']
        
        return transformed_df

def main(raw_df: pd.DataFrame, country: str, retailer: str):
    transformation = NotinoTransformation(country=country, retailer=retailer)
    transformed_df = transformation.transform_data(raw_df)
    return transformed_df

if __name__ == "__main__":
    raw_df = pd.read_csv("notino_raw.csv")
    transformed_df = main(raw_df, country="UK", retailer="notino")
    transformed_df.to_csv("notino_transformed.csv", index=False)