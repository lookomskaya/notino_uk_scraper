import pandas as pd
from bs4 import BeautifulSoup
import time
from datetime import datetime

# Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class AbstractScraper(ABC):
    def __init__(self):
        self.retailer = 'Notino'
        self.country = 'UK'
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f'Initialized scraper for {self.retailer} in {self.country}')
    
    @abstractmethod
    def get(self, url, **kwargs):
        pass

    @abstractmethod
    def post(self, url, **kwargs):
        pass

class NotinoScraper(AbstractScraper):
    def get(self, url, **kwargs):
        headers = kwargs.get('headers', {})
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        })
        kwargs['headers'] = headers
        self.logger.info(f'Sending GET request to {url} with params {kwargs}')
        try:
            response = requests.get(url, **kwargs)
            response.raise_for_status()
            self.logger.info(f'Successful GET request to {url}')
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f'Error during GET request to {url}: {e}')
            return None

    def post(self, url, **kwargs):
        headers = kwargs.get('headers', {})
        headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0'
        })
        kwargs['headers'] = headers
        self.logger.info(f'Sending POST request to {url} with params {kwargs}')
        try:
            response = requests.post(url, **kwargs)
            response.raise_for_status()
            self.logger.info(f'Successful POST request to {url}')
            return response
        except requests.exceptions.RequestException as e:
            self.logger.error(f'Error during POST request to {url}: {e}')
            return None

    def scrape_page(self, url):
        max_retries = 3
        for attempt in range(max_retries):
            response = self.get(url)
            if response:
                # Parsing of HTML content
                soup = BeautifulSoup(response.content, 'html.parser')

                # Checking HTML structure and logging
                self.logger.debug(f"HTML content of {url}:\n{soup.prettify()[:1000]}")  

                # Gathering information about the products
                products = soup.find_all('div', {'data-testid': 'product-container'})
                self.logger.debug(f"Found {len(products)} products on {url}")

                if products:
                    data = []
                    for product in products:
                        try:
                            # Scraping name and brand of products
                            name_element = product.find('h2', class_='sc-guDLey')
                            name = name_element.text.strip() if name_element else 'N/A'
                            
                            brand_element = product.find('h3', class_='sc-dmyCSP')
                            brand = brand_element.text.strip() if brand_element else 'N/A'
                            
                            # Scraping description
                            description_element = product.find('p', class_='sc-FjMCv')
                            description = description_element.text.strip() if description_element else 'N/A'
                            
                            # Scraping price
                            price_element = product.find('span', {'data-testid': 'price-component'})
                            price = price_element.text.strip() if price_element else 'N/A'
                            
                            # Scraping URL
                            product_url_element = product.find('a', href=True)
                            product_url = product_url_element['href'] if product_url_element else 'N/A'
                            full_product_url = f"https://www.notino.co.uk{product_url}"
                            
                            # Scraping URL image
                            image_element = product.find('img', {'src': True})
                            image_url = image_element['src'] if image_element else 'N/A'
                            
                            # Scraping discounts
                            discount_elements = product.find_all('span', class_='styled__DiscountValue-sc-1b3ggfp-1 jWXmOz')
                            discounts = ', '.join([el.text.strip() for el in discount_elements]) if discount_elements else 'N/A'
                            
                            # Scraping promocodes
                            promo_code_elements = product.find_all('span', class_='styled__StyledDiscountCode-sc-1i2ozu3-1 gfxrfw')
                            promo_codes = ', '.join([el.text.strip() for el in promo_code_elements]) if promo_code_elements else 'N/A'
                            
                            data.append({
                                'Product Name': name,
                                'Brand': brand,
                                'Description': description,
                                'Price': price,
                                'URL': full_product_url,
                                'Image URL': image_url,
                                'Discount': discounts,
                                'Promo Code': promo_codes
                            })
                        except Exception as e:
                            self.logger.error(f"Error processing product: {e}")
                    return data

            self.logger.warning(f"Retrying ({attempt + 1}/{max_retries})...")
            time.sleep(2)  # Delay before retry

        self.logger.error("Failed to retrieve valid data after multiple attempts.")
        return []

    def scrape_toothpastes(self):
        base_url = "https://www.notino.co.uk/toothpaste/"
        all_data = []
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Impmlement range for going page to page
        for x in range(1, 22):  # From 1 to 21 page including
            self.logger.info(f'Scraping page {x}')
            url = f"{base_url}?page={x}"
            page_data = self.scrape_page(url)
            if not page_data:
                self.logger.warning(f"No data found on page {x}. Continuing to next page.")
                continue  # Going to next page
            
            # Adding timestamp for scraping
            for product in page_data:
                product['Scraping Timestamp'] = timestamp
            
            all_data.extend(page_data)
        
        if not all_data:
            self.logger.error("No product data extracted.")
            return None

        # Creating DataFrame and saving data to CSV
        df = pd.DataFrame(all_data)
        df.to_csv('notino_raw.csv', index=False)
        self.logger.info("Data scraped and saved to notino_raw.csv")
        return df

def main() -> pd.DataFrame:
    scraper = NotinoScraper()
    products_df = scraper.scrape_toothpastes()

    return products_df

if __name__ == "__main__":
    df_raw = main()
    if df_raw is not None:
        df_raw.to_csv("notino_raw.csv", index=False)