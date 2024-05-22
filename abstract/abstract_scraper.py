from abc import ABC, abstractmethod
import requests
import logging

# Logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levellevelname)s - %(message)s')

class AbstractScraper(ABC):
    def __init__(self, retailer, country):
        self.retailer = retailer
        self.country = country
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.debug(f'Initialized scraper for {self.retailer} in {self.country}')
    
    @abstractmethod
    def get(self, url, **kwargs):
        pass

    @abstractmethod
    def post(self, url, **kwargs):
        pass