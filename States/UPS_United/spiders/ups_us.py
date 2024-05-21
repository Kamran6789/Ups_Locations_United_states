import requests
import scrapy
from scrapy.crawler import CrawlerProcess


class UpsUsSpider(scrapy.Spider):
    name = "ups_us"
    start_urls = ["https://locations.ups.com/us/en/"]

    def parse(self, response):
        print("Parsing states...")
        state_names = response.css(".ga-link::text").getall()
        state_links = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a::attr(href)").getall()
        for state_name, state_link in zip(state_names, state_links):
            yield response.follow(state_link, self.parse_counties, meta={'state': state_name})

    def parse_counties(self, response):
        print("Parsing counties...")
        state_name = response.meta['state']
        county_names = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a > span::text").getall()  # county name
        county_links = response.css(
            "#main-container > div > div:nth-child(3) > div > div > div > div > ul > li > div > a::attr(href)").getall()
        for county_name, county_link in zip(county_names, county_links):
            county_population = self.get_population(state_name, county_name)
            yield response.follow(county_link, self.parse_details,
                                  meta={'state': state_name, 'county': county_name, 'population': county_population})

    def parse_details(self, response):
        print("Parsing details...")
        state_name = response.meta['state']
        county_name = response.meta['county']
        county_population = response.meta['population']
        center_names = response.css(".location-name::text")[::4].getall()
        total_center = len(center_names)
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
                'State': state_name,
                'County': county_name,
                'Population': county_population,
                'Center_name': center_name,
                'Address': address,
                'Contact': contact,
                'UPS Access Points': total_center
            }

    def get_population(self, state_name, county_name):
        print(f"Getting population for {county_name} in {state_name}...")
        url = "https://api.census.gov/data/2020/dec/ddhca?get=NAME&T01001_001N&for=place:*&key=1c7bfc14ea5e6043e9494fb7975ff55b57d2566b"
        params = {
            'get': 'NAME,POP',
            'for': f'county:*',
            'in': f'state:{self.get_state_fips(state_name)}'
        }
        response = requests.get(url, params=params)
        data = response.json()
        for county_data in data[1:]:
            if county_data[0].startswith(county_name):
                return county_data[1]
        return "Population Data Not Found"

    def get_state_fips(self, state_name):
        # A dictionary mapping state names to their FIPS codes
        state_fips = {
            "Alabama": "01",
            "Alaska": "02",
            "Arizona": "04",
            "Arkansas": "05",
            "California": "06",
            "Colorado": "08",
            "Connecticut": "09",
            "Delaware": "10",
            "Florida": "12",
            "Georgia": "13",
            "Hawaii": "15",
            "Idaho": "16",
            "Illinois": "17",
            "Indiana": "18",
            "Iowa": "19",
            "Kansas": "20",
            "Kentucky": "21",
            "Louisiana": "22",
            "Maine": "23",
            "Maryland": "24",
            "Massachusetts": "25",
            "Michigan": "26",
            "Minnesota": "27",
            "Mississippi": "28",
            "Missouri": "29",
            "Montana": "30",
            "Nebraska": "31",
            "Nevada": "32",
            "New Hampshire": "33",
            "New Jersey": "34",
            "New Mexico": "35",
            "New York": "36",
            "North Carolina": "37",
            "North Dakota": "38",
            "Ohio": "39",
            "Oklahoma": "40",
            "Oregon": "41",
            "Pennsylvania": "42",
            "Rhode Island": "44",
            "South Carolina": "45",
            "South Dakota": "46",
            "Tennessee": "47",
            "Texas": "48",
            "Utah": "49",
            "Vermont": "50",
            "Virginia": "51",
            "Washington": "53",
            "West Virginia": "54",
            "Wisconsin": "55",
            "Wyoming": "56",
        }
        return state_fips.get(state_name)


if __name__ == '__main__':
    # Configure the settings for Scrapy to output to a CSV file
    process = CrawlerProcess(settings={
        "FEEDS": {
            "Ups_USA.csv": {
                "format": "csv",
                "encoding": "utf8",
                "store_empty": False,
                "fields": ["State", "County", "Population", "Center_name", "Address", "Contact",'UPS Access Points'],
            },
        },
    })

    process.crawl(UpsUsSpider)
    process.start()
