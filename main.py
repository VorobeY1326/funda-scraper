import sqlite3
from sqlite3 import OperationalError
import json
from telegram import Bot, constants
import argparse
import asyncio

from funda_scraper import FundaScraper

from geoapify import Geoapify


TABLE_NAME = "houses"

def update_houses_db():
    ctx = sqlite3.connect("db/listings.db")

    try:
        known_urls = set([row[0] for row in ctx.execute(f"SELECT url FROM {TABLE_NAME}").fetchall()])
    except OperationalError:
        known_urls = set()

    scraper = FundaScraper(
        area="amsterdam,5km",
        want_to="buy",
        find_past=False,
        page_start=1,
        n_pages=3,
        min_price=300000,
        max_price=400000,
        min_floor_area=40,
        extra_args={
            'energy_label': '%5B%22A%2B%2B%2B%2B%2B%22,%22A%2B%2B%2B%2B%22,%22A%2B%2B%2B%22,%22A%2B%2B%22,%22A%2B%22,%22A%22,%22B%22,%22C%22%5D',
            'sort': '%22date_down%22'
        },
        known_urls=known_urls,
    )

    df = scraper.run(raw_data=False, save=False)

    if not df.empty:
        df["notification_sent"] = 0
        df.set_index("house_id")

        df.to_sql(name=TABLE_NAME, con=ctx, index=False, if_exists="append")
        ctx.commit()

    ctx.close()


def format_message(row):
    return f"""
<b>{row['address']}</b>
💶 {row['price']:,}
🏠 {row['living_area']} m2
🚪 {row['room']} 🛏️ {row['bedroom']}
⚡️ {row['energy_label']}
⏳ {row['year_built']}

{row['url']}
"""


async def send_new_houses_to_telegram():
    ctx = sqlite3.connect("db/listings.db")
    ctx.row_factory = sqlite3.Row

    new_entries = ctx.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE notification_sent=0"
    ).fetchall()

    if len(new_entries) > 5:
        new_entries = new_entries[0:5]
        print(f"Truncated to 5 out of {len(new_entries)} records")

    with open("telegram_config.json", "r") as f:
        config = json.load(f)

    token = config["api_key"]
    groupId = config["group_id"]

    geoapify = Geoapify()

    async with Bot(token) as bot:    
        for entry in new_entries:
            print(entry['house_id'])
            print("Sending message")
            try:
                coordinates = geoapify.get_coordinates(entry['address'], entry['zip'])
                map_image = geoapify.get_amsterdam_center_with_marker(coordinates)
                await bot.send_photo(chat_id=groupId, photo=map_image, caption=format_message(entry),
                                     parse_mode=constants.ParseMode.HTML)
            except Exception as e:
                print('Failed with image generation, sending normal text')
                print(e)
                await bot.send_message(text=format_message(entry), chat_id=groupId, parse_mode=constants.ParseMode.HTML)
            print("Message sent")

            ctx.execute(f"UPDATE {TABLE_NAME} SET notification_sent=1 WHERE house_id='{entry['house_id']}'")
            ctx.commit()

    ctx.close()


def main():
    parser = argparse.ArgumentParser(description="Get housing updates and send notifications to Telegram")
    parser.add_argument("--update", action="store_true", help="Update the houses database.")
    parser.add_argument("--send", action="store_true", help="Send new houses to Telegram.")

    args = parser.parse_args()

    if args.update:
        update_houses_db()

    if args.send:
        asyncio.run(send_new_houses_to_telegram())

if __name__ == "__main__":
    main()
