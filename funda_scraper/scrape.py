"""Main funda scraper module"""

import argparse
import datetime
import json
import multiprocessing as mp
import os
from collections import OrderedDict
from typing import List, Optional
from urllib.parse import urlparse, urlunparse

import urllib3
import pandas as pd
from curl_cffi import requests
import re
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import process_map

from funda_scraper.config.core import config
from funda_scraper.preprocess import clean_date_format, preprocess_data
from funda_scraper.utils import logger


class FundaScraper(object):
    """
    A class used to scrape real estate data from the Funda website.
    """

    def __init__(
        self,
        area: str,
        want_to: str,
        page_start: int = 1,
        n_pages: int = 1,
        find_past: bool = False,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        property_type: Optional[str] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
        extra_args: Optional[dict[str, str]] = None,
        known_urls: Optional[list[str]] = None
    ):
        """

        :param area: The area to search for properties, formatted for URL compatibility.
        :param want_to: Specifies whether the user wants to buy or rent properties.
        :param page_start: The starting page number for the search.
        :param n_pages: The number of pages to scrape.
        :param find_past: Flag to indicate whether to find past listings.
        :param min_price: The minimum price for the property search.
        :param max_price: The maximum price for the property search.
        :param days_since: The maximum number of days since the listing was published.
        :param property_type: The type of property to search for.
        :param min_floor_area: The minimum floor area for the property search.
        :param max_floor_area: The maximum floor area for the property search.
        :param sort: The sorting criterion for the search results.
        """
        # Init attributes
        self.area = area.lower().replace(" ", "-")
        self.property_type = property_type
        self.want_to = want_to
        self.find_past = find_past
        self.page_start = max(page_start, 1)
        self.n_pages = max(n_pages, 1)
        self.page_end = self.page_start + self.n_pages - 1
        self.min_price = min_price
        self.max_price = max_price
        self.days_since = days_since
        self.min_floor_area = min_floor_area
        self.max_floor_area = max_floor_area
        self.sort = sort
        self.extra_args = extra_args
        self.known_urls = known_urls if known_urls is not None else []

        # Instantiate along the way
        self.links: List[str] = []
        self.raw_df = pd.DataFrame()
        self.clean_df = pd.DataFrame()
        self.base_url = config.base_url
        self.selectors = config.css_selector

    def __repr__(self):
        return (
            f"FundaScraper(area={self.area}, "
            f"want_to={self.want_to}, "
            f"n_pages={self.n_pages}, "
            f"page_start={self.page_start}, "
            f"find_past={self.find_past}, "
            f"min_price={self.min_price}, "
            f"max_price={self.max_price}, "
            f"days_since={self.days_since}, "
            f"min_floor_area={self.min_floor_area}, "
            f"max_floor_area={self.max_floor_area}, "
            f"find_past={self.find_past})"
            f"min_price={self.min_price})"
            f"max_price={self.max_price})"
            f"days_since={self.days_since})"
            f"sort={self.sort})"
        )

    @property
    def to_buy(self) -> bool:
        """Determines if the search is for buying or renting properties."""
        if self.want_to.lower() in ["buy", "koop", "b", "k"]:
            return True
        elif self.want_to.lower() in ["rent", "huur", "r", "h"]:
            return False
        else:
            raise ValueError("'want_to' must be either 'buy' or 'rent'.")

    @property
    def check_days_since(self) -> int:
        """Validates the 'days_since' attribute."""
        if self.find_past:
            raise ValueError("'days_since' can only be specified when find_past=False.")

        if self.days_since in [None, 1, 3, 5, 10, 30]:
            return self.days_since
        else:
            raise ValueError("'days_since' must be either None, 1, 3, 5, 10 or 30.")

    @property
    def check_sort(self) -> str:
        """Validates the 'sort' attribute."""
        if self.sort in [
            None,
            "relevancy",
            "date_down",
            "date_up",
            "price_up",
            "price_down",
            "floor_area_down",
            "plot_area_down",
            "city_up" "postal_code_up",
        ]:
            return self.sort
        else:
            raise ValueError(
                "'sort' must be either None, 'relevancy', 'date_down', 'date_up', 'price_up', 'price_down', "
                "'floor_area_down', 'plot_area_down', 'city_up' or 'postal_code_up'. "
            )

    @staticmethod
    def _check_dir() -> None:
        """Ensures the existence of the directory for storing data."""
        if not os.path.exists("data"):
            os.makedirs("data")

    @staticmethod
    def _get_links_from_one_parent(url: str) -> List[str]:
        """Scrapes all available property links from a single Funda search page."""
        response = requests.get(url, headers=config.header, impersonate="chrome110")
        soup = BeautifulSoup(response.text, "lxml")

        script_tag = soup.find_all("script", {"type": "application/ld+json"})[0]
        json_data = json.loads(script_tag.contents[0])
        urls = [item["url"] for item in json_data["itemListElement"]]
        return urls

    def reset(
        self,
        area: Optional[str] = None,
        property_type: Optional[str] = None,
        want_to: Optional[str] = None,
        page_start: Optional[int] = None,
        n_pages: Optional[int] = None,
        find_past: Optional[bool] = None,
        min_price: Optional[int] = None,
        max_price: Optional[int] = None,
        days_since: Optional[int] = None,
        min_floor_area: Optional[str] = None,
        max_floor_area: Optional[str] = None,
        sort: Optional[str] = None,
        extra_args: Optional[dict[str,str]] = None,
    ) -> None:
        """Resets or initializes the search parameters."""
        if area is not None:
            self.area = area
        if property_type is not None:
            self.property_type = property_type
        if want_to is not None:
            self.want_to = want_to
        if page_start is not None:
            self.page_start = max(page_start, 1)
        if n_pages is not None:
            self.n_pages = max(n_pages, 1)
        if find_past is not None:
            self.find_past = find_past
        if min_price is not None:
            self.min_price = min_price
        if max_price is not None:
            self.max_price = max_price
        if days_since is not None:
            self.days_since = days_since
        if min_floor_area is not None:
            self.min_floor_area = min_floor_area
        if max_floor_area is not None:
            self.max_floor_area = max_floor_area
        if sort is not None:
            self.sort = sort
        if extra_args is not None:
            self.extra_args = extra_args

    @staticmethod
    def remove_duplicates(lst: List[str]) -> List[str]:
        """Removes duplicate links from a list."""
        return list(OrderedDict.fromkeys(lst))

    @staticmethod
    def fix_link(link: str) -> str:
        """Fixes double language prefixes in links."""
        return link.replace("/en/en/", "/en/")

    def fetch_all_links(self, page_start: int = None, n_pages: int = None) -> None:
        """Collects all available property links across multiple pages."""

        page_start = self.page_start if page_start is None else page_start
        n_pages = self.n_pages if n_pages is None else n_pages

        logger.info("*** Phase 1: Fetch all the available links from all pages *** ")
        urls = []
        main_url = self._build_main_query_url()

        for i in tqdm(range(page_start, page_start + n_pages)):
            try:
                item_list = self._get_links_from_one_parent(
                    f"{main_url}&search_result={i}"
                )
                urls += item_list
            except IndexError:
                self.page_end = i
                logger.info(f"*** The last available page is {self.page_end} ***")
                break

        urls = self.remove_duplicates(urls)
        fixed_urls = [self.fix_link(url) for url in urls]

        new_urls = list(set(fixed_urls) - set(self.known_urls))

        logger.info(
            f"*** Got all the urls. {len(new_urls)} new houses found from {self.page_start} to {self.page_end} ***"
        )
        self.links = new_urls

    def _build_main_query_url(self) -> str:
        """Constructs the main query URL for the search."""
        query = "koop" if self.to_buy else "huur"

        if self.area.startswith("["):          # list
            # Use the value as‑is (the scraper will still URL‑encode the
            # *extra_args* later, but we deliberately skip the wrapper.)
            area_part = f"{self.area}"
        else:
            # Default behaviour – treat the supplied string as a plain area name
            area_part = f"%5B%22{self.area}%22%5D"

        main_url = (
            f"{self.base_url}/zoeken/{query}?selected_area={area_part}"
        )

        if self.property_type:
            property_types = self.property_type.split(",")
            formatted_property_types = [
                "%22" + prop_type + "%22" for prop_type in property_types
            ]
            main_url += f"&object_type=%5B{','.join(formatted_property_types)}%5D"

        if self.find_past:
            main_url = f'{main_url}&availability=%5B"unavailable"%5D'

        if self.min_price is not None or self.max_price is not None:
            min_price = "" if self.min_price is None else self.min_price
            max_price = "" if self.max_price is None else self.max_price
            main_url = f"{main_url}&price=%22{min_price}-{max_price}%22"

        if self.days_since is not None:
            main_url = f"{main_url}&publication_date={self.check_days_since}"

        if self.min_floor_area or self.max_floor_area:
            min_floor_area = "" if self.min_floor_area is None else self.min_floor_area
            max_floor_area = "" if self.max_floor_area is None else self.max_floor_area
            main_url = f"{main_url}&floor_area=%22{min_floor_area}-{max_floor_area}%22"

        if self.sort is not None:
            main_url = f"{main_url}&sort=%22{self.check_sort}%22"

        if self.extra_args:
            for key, value in self.extra_args.items():
                main_url = f"{main_url}&{key}={value}"

        main_url = urllib3.util.parse_url(main_url).url

        logger.info(f"*** Main URL: {main_url} ***")
        return main_url

    @staticmethod
    def get_value_from_css(soup: BeautifulSoup, selector: str) -> str:
        """Extracts data from HTML using a CSS selector."""
        result = soup.select(selector)
        if len(result) > 0:
            result = result[0].text
        else:
            result = "na"
        return result

    def scrape_one_link(self, link: str) -> List[str]:
        """Scrapes data from a single property link."""
        response = requests.get(link, headers=config.header, impersonate="chrome110")
        soup = BeautifulSoup(response.text, "lxml")

        # 1. Parse JSON-LD
        json_ld_data = {}
        breadcrumbs = []
        script_tags = soup.find_all("script", {"type": "application/ld+json"})
        for tag in script_tags:
            try:
                js = json.loads(tag.contents[0])
                if isinstance(js, list):
                    js = js[0]
                if js.get("@type") == "BreadcrumbList" or (isinstance(js.get("@type"), list) and "BreadcrumbList" in js.get("@type")):
                    breadcrumbs = [item["item"]["name"] for item in js.get("itemListElement", []) if "item" in item]
                elif "Apartment" in js.get("@type", []) or "Product" in js.get("@type", []) or "House" in js.get("@type", []):
                    json_ld_data = js
            except Exception as e:
                pass

        # 2. Parse DL/DT/DD characteristics
        characteristics = {}
        for dl in soup.find_all("dl"):
            for dt, dd in zip(dl.find_all("dt"), dl.find_all("dd")):
                key = dt.text.strip().lower()
                val = dd.text.strip()
                characteristics[key] = val

        # Helper to search keys in characteristics
        def get_char(keys):
            for k in keys:
                if k.lower() in characteristics:
                    return characteristics[k.lower()]
            return "na"

        # 3. Extract core fields from JSON-LD
        price_val = "na"
        if json_ld_data and "offers" in json_ld_data:
            price_val = f"€ {json_ld_data['offers'].get('price', '')}"

        address_val = "na"
        if json_ld_data:
            address_val = json_ld_data.get("name", "na")

        # Photos
        photos_list = []
        if json_ld_data and "photo" in json_ld_data:
            for photo_obj in json_ld_data["photo"]:
                if isinstance(photo_obj, dict) and "contentUrl" in photo_obj:
                    photos_list.append(photo_obj["contentUrl"])
        photos_string = ", ".join(photos_list)

        # 4. Zip code
        zip_code_val = "na"
        h1 = soup.find("h1")
        if h1:
            match = re.search(r"(\d{4}\s?[A-Z]{2})", h1.text)
            if match:
                zip_code_val = match.group(1)

        # Neighborhood from breadcrumbs
        neighborhood_name_val = "na"
        if len(breadcrumbs) >= 3:
            neighborhood_name_val = breadcrumbs[2]

        # Description
        descrip_val = "na"
        desc_heading = soup.find(lambda tag: tag.name in ['h2', 'h3'] and tag.text.strip().lower() in ['description', 'omschrijving'])
        if desc_heading:
            sibling = desc_heading.find_next_sibling()
            if sibling:
                descrip_val = sibling.text.strip()

        # Listed since
        listed_since_val = get_char(["Listed since", "Aangeboden sinds"])

        # Size & Living area
        size_val = get_char(["Living area", "Woonoppervlakte"])
        living_area_val = size_val

        # Year built
        year_val = get_char(["Year of construction", "Bouwjaar"])

        # Kind of house
        kind_of_house_val = get_char(["Type apartment", "Type woning", "Type house", "Soort appartement", "Soort woonhuis"])

        # Building type
        building_type_val = get_char(["Building type", "Bouwvorm"])

        # Rooms & Bathrooms
        num_of_rooms_val = get_char(["Number of rooms", "Aantal kamers"])
        num_of_bathrooms_val = get_char(["Number of bath rooms", "Aantal badkamers"])

        # Layout
        layout_val = get_char(["Layout", "Indeling"])

        # Energy label
        energy_label_val = get_char(["Energy label", "Energielabel"])

        # Insulation
        insulation_val = get_char(["Insulation", "Isolatie"])

        # Heating
        heating_val = get_char(["Heating", "Verwarming"])

        # Ownership
        ownership_val = get_char(["Ownership situation", "Eigendomssituatie", "Ligging"])

        # Exteriors & Parking
        exteriors_val = get_char(["Balcony/roof garden", "Balkon/dakterras", "Buitenruimte"])
        parking_val = get_char(["Type of parking facilities", "Parkeerfaciliteiten", "Garage"])

        # Historical parameters (sold data)
        date_list_val = get_char(["Date of list", "Aanmeldingsdatum"])
        date_sold_val = get_char(["Date of sale", "Verkoopdatum"])
        term_val = get_char(["Term", "Looptijd"])
        price_sold_val = get_char(["Selling price", "Verkoopprijs"])
        last_ask_price_val = price_val
        last_ask_price_m2_val = get_char(["Asking price per m²", "Asking price per m?", "Vraagprijs per m²"])

        # Map to scraper results structure
        result = [
            link,
            price_val,
            address_val,
            descrip_val,
            listed_since_val,
            zip_code_val,
            size_val,
            year_val,
            living_area_val,
            kind_of_house_val,
            building_type_val,
            num_of_rooms_val,
            num_of_bathrooms_val,
            layout_val,
            energy_label_val,
            insulation_val,
            heating_val,
            ownership_val,
            exteriors_val,
            parking_val,
            neighborhood_name_val,
            date_list_val,
            date_sold_val,
            term_val,
            price_sold_val,
            last_ask_price_val,
            last_ask_price_m2_val,
        ]

        # Clean up text
        result = [str(r).replace("\n", "").replace("\r", "").strip() for r in result]
        result.append(photos_string)
        return result

    def scrape_pages(self) -> None:
        """Scrapes data from all collected property links."""

        logger.info("*** Phase 2: Start scraping from individual links ***")
        df = pd.DataFrame({key: [] for key in self.selectors.keys()})

        # Scrape pages with multiprocessing to improve efficiency
        # TODO: use asyncio instead
        pools = mp.cpu_count()
        content = process_map(self.scrape_one_link, self.links, max_workers=pools)

        for i, c in enumerate(content):
            df.loc[len(df)] = c

        def get_city_from_url(url):
            parts = url.split("/")
            if "koop" in parts:
                return parts[parts.index("koop") + 1]
            elif "huur" in parts:
                return parts[parts.index("huur") + 1]
            return "na"

        df["city"] = df["url"].map(get_city_from_url)
        df["log_id"] = datetime.datetime.now().strftime("%Y%m-%d%H-%M%S")
        if not self.find_past:
            df = df.drop(["term", "price_sold", "date_sold"], axis=1)
        logger.info(f"*** All scraping done: {df.shape[0]} results ***")
        self.raw_df = df

    def save_csv(self, df: pd.DataFrame, filepath: str = None) -> None:
        """Saves the scraped data to a CSV file."""
        if filepath is None:
            self._check_dir()
            date = str(datetime.datetime.now().date()).replace("-", "")
            status = "unavailable" if self.find_past else "unavailable"
            want_to = "buy" if self.to_buy else "rent"
            filepath = f"./data/houseprice_{date}_{self.area}_{want_to}_{status}_{len(self.links)}.csv"
        df.to_csv(filepath, index=False)
        logger.info(f"*** File saved: {filepath}. ***")

    def run(
        self, raw_data: bool = False, save: bool = False, filepath: str = None
    ) -> pd.DataFrame:
        """
        Runs the full scraping process, optionally saving the results to a CSV file.

        :param raw_data: if true, the data won't be pre-processed
        :param save: if true, the data will be saved as a csv file
        :param filepath: the name for the file
        :return: the (pre-processed) dataframe from scraping
        """
        self.fetch_all_links()
        if not self.links:
            logger.info("*** No new links found. ***")
            return pd.DataFrame()
        self.scrape_pages()

        if raw_data:
            df = self.raw_df
        else:
            logger.info("*** Cleaning data ***")
            df = preprocess_data(df=self.raw_df, is_past=self.find_past)
            self.clean_df = df

        if save:
            self.save_csv(df, filepath)

        logger.info("*** Done! ***")
        return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--area",
        type=str,
        help="Specify which area you are looking for",
        default="amsterdam",
    )
    parser.add_argument(
        "--want_to",
        type=str,
        help="Specify you want to 'rent' or 'buy'",
        default="rent",
        choices=["rent", "buy"],
    )
    parser.add_argument(
        "--find_past",
        action="store_true",
        help="Indicate whether you want to use historical data",
    )
    parser.add_argument(
        "--page_start", type=int, help="Specify which page to start scraping", default=1
    )
    parser.add_argument(
        "--n_pages", type=int, help="Specify how many pages to scrape", default=1
    )
    parser.add_argument(
        "--min_price", type=int, help="Specify the min price", default=None
    )
    parser.add_argument(
        "--max_price", type=int, help="Specify the max price", default=None
    )
    parser.add_argument(
        "--days_since",
        type=int,
        help="Specify the days since publication",
        default=None,
    )
    parser.add_argument(
        "--sort",
        type=str,
        help="Specify sorting",
        default=None,
        choices=[
            None,
            "relevancy",
            "date_down",
            "date_up",
            "price_up",
            "price_down",
            "floor_area_down",
            "plot_area_down",
            "city_up" "postal_code_up",
        ],
    )
    parser.add_argument(
        "--raw_data",
        action="store_true",
        help="Indicate whether you want the raw scraping result",
    )
    parser.add_argument(
        "--save",
        action="store_true",
        help="Indicate whether you want to save the data",
    )

    args = parser.parse_args()
    scraper = FundaScraper(
        area=args.area,
        want_to=args.want_to,
        find_past=args.find_past,
        page_start=args.page_start,
        n_pages=args.n_pages,
        min_price=args.min_price,
        max_price=args.max_price,
        days_since=args.days_since,
        sort=args.sort,
    )
    df = scraper.run(raw_data=args.raw_data, save=args.save)
    print(df.head())
