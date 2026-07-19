import sqlite3
from sqlite3 import OperationalError
import json
from telegram import Bot, constants
import argparse
import asyncio

from funda_scraper import FundaScraper

from geoapify import Geoapify
from transitous import Transitous, TransitousTravelTimeResult
from areas import Areas, AreaType


TABLE_NAME = "houses"

def update_houses_db():
    ctx = sqlite3.connect("db/listings.db")

    try:
        known_urls = set([row[0] for row in ctx.execute(f"SELECT url FROM {TABLE_NAME}").fetchall()])
    except OperationalError:
        known_urls = set()

    scraper = FundaScraper(
        area="",
        want_to="buy",
        find_past=False,
        page_start=1,
        n_pages=3,
        extra_args={
            'price': '%22370000-500000%22',
            'bedrooms': '%222-%22',
            'energy_label': '[%22A%2B%2B%2B%2B%22,%22A%2B%2B%2B%22,%22A%2B%2B%22,%22A%2B%22,%22A%22,%22B%22,%22C%22,%22A%2B%2B%2B%2B%2B%22]',
            # 'construction_period': '[%22from_2001_to_2010%22,%22from_2011_to_2020%22,%22after_2020%22,%22from_1991_to_2000%22]',
            'object_type': '[%22apartment%22,%22house%22]',
            'construction_type': '[%22resale%22]',
            'sort': '%22date_down%22',
            'custom_area': '%7Dfh%5E%7Bat%7DHdPueRu_N%7BF%7DvAptTlfP_fA,krr%5D_dl~H%7Ba@jrFwrG%7DBdIg%7BDlkHer@,%7D%60p%5Dqli~Hoy@jcD~uJteBtqGmbIwoHiaBkxCujDkmGrwChhCvlD,mf%7C%5Cwuu~HzgFkmE%7B%60CkhB%7DeSbfGl%7DAnqAn%60M%7B%60@,_%7Dq%5Cyhm~HoqFgDgjB~w@~eEduMziQ_nDcsL%7DyI,k%7Ds%5Cep%7C~H%60eTutAsoBqdHcfNrvBaqDz_EvbBvaA,%7Bv%7C%5Csnk_IqfEyuGoxOjiC~yFpiG%60eNc%7DB'
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


def format_message(row, travel_time: TransitousTravelTimeResult, area_type: AreaType) -> str:
    if area_type == AreaType.GREEN:
        area_marker = '🟢 '
    elif area_type == AreaType.ORANGE:
        area_marker = '🟠 '
    else:
        area_marker = ''
    return f"""
{area_marker}<b>{row['address']}</b>
💶 {row['price']:,}
🏠 {row['living_area']} m2
🚪 {row['room']} 🛏️ {row['bedroom']}
⚡️ {row['energy_label']}
⏳ {row['year_built']}
{travel_time.travel_modes_emojis} {travel_time.travel_time_min}-{travel_time.travel_time_max}

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
    transitous = Transitous()
    areas = Areas()

    async with Bot(token) as bot:    
        for entry in new_entries:
            print(entry['house_id'])
            print("Sending message")
            try:
                coordinates = geoapify.get_coordinates(entry['address'], entry['zip'])
                map_image = geoapify.get_amsterdam_center_with_marker(coordinates)
                travel_time = transitous.get_travel_time_to_work(coordinates[0], coordinates[1])
                area_type = areas.get_area_type(coordinates[0], coordinates[1])
                await bot.send_photo(chat_id=groupId, photo=map_image, caption=format_message(entry, travel_time, area_type),
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
