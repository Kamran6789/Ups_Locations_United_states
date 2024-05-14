import scrapy
from scrapy.crawler import CrawlerProcess
import pandas as pd


class UpsUsSpider(scrapy.Spider):
    name = "ups_us"
    start_urls = ["https://locations.ups.com/us/en/"]

    def __init__(self, csv_file='state_populations.csv'):
        # Read the CSV file and create a dictionary for county populations
        df = pd.read_csv(csv_file, sep=',')
        self.county_populations = df.set_index('NAME')['P1_001N'].to_dict()

        # Debugging: Print out the first few entries in the dictionary
        print("Loaded county populations:", list(self.county_populations.items())[5])

    def parse(self, response):
        state_names = response.css(".ga-link::text")[5].getall()
        state_links = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a::attr(href)")[
                      5].getall()
        for state_name, state_link in zip(state_names, state_links):
            yield response.follow(state_link, self.parse_counties, meta={'state': state_name})

    def parse_counties(self, response):
        state_name = response.meta['state']
        county_names = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a > span::text").getall()  # county name
        county_links = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a::attr(href)").getall()
        for county_name, county_link in zip(county_names, county_links):
            # Normalize the county name to match the CSV format
            county_key = f"{county_name} County, {state_name}".strip()

            # Debugging: Print out the keys being generated and the corresponding population
            print(f"Generated key: {county_key}")

            county_population = self.county_populations.get(county_key, "Population Not Found")

            # Debugging: Print the population found
            print(f"Population for {county_key}: {county_population}")

            yield response.follow(county_link, self.parse_details,
                                  meta={'state': state_name, 'county': county_name, 'population': county_population})

    def parse_details(self, response):
        state_name = response.meta['state']
        county_name = response.meta['county']
        county_population = response.meta['population']
        center_names = response.css(".location-name::text")[::6].getall()
        address_parts = response.css(".address div::text,.location-name strong::text").getall()
        addresses = [
            ''.join(address_parts[i:i + 2]) + " inside: " + ''.join(address_parts[i + 2:i + 5])
            for i in range(0, len(address_parts), 5)
        ]
        contacts = response.css(".phone::text").getall()
        while len(contacts) < len(center_names):
            contacts.append('Not given on that place')

        for center_name, address, contact in zip(center_names, addresses, contacts):
            yield {
                'state': state_name,
                'county': county_name,
                'population': county_population,
                'center_name': center_name,
                'address': address,
                'contact': contact
            }


if __name__ == '__main__':
    # Configure the settings for Scrapy to output to a JSON file
    process = CrawlerProcess(settings={
        "FEEDS": {
            "Ups_US.json": {
                "format": "json",
                "encoding": "utf8",
                "store_empty": False,
                "indent": 4,
            },
        },
    })

    process.crawl(UpsUsSpider)
    process.start()


