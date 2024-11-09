import sqlite3
from sqlite3 import OperationalError
import json
from telegram import Bot

from funda_scraper import FundaScraper

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
        max_price=425000,
        extra_args={},
        known_urls=known_urls,
    )

    df = scraper.run(raw_data=False, save=False)

    if not df.empty:
        df["notification_sent"] = 0
        df.set_index("house_id")

        df.to_sql(name=TABLE_NAME, con=ctx, index=False, if_exists="append")
        ctx.commit()

    ctx.close()


async def send_new_houses_to_telegram():
    ctx = sqlite3.connect("db/listings.db")
    ctx.row_factory = sqlite3.Row

    new_entries = ctx.execute(
        f"SELECT * FROM {TABLE_NAME} WHERE notification_sent=0"
    ).fetchall()

    new_entries = new_entries[0:1]

    with open("telegram_config.json", "r") as f:
        config = json.load(f)

    token = config["api_key"]
    groupId = config["group_id"]

    async with Bot(token) as bot:    
        for entry in new_entries:
            print(entry['house_id'])
            print("Sending message")
            await bot.send_message(text="TEST", chat_id=groupId)
            print("Message sent")

            ctx.execute(f"UPDATE {TABLE_NAME} SET notification_sent=1 WHERE house_id='{entry['house_id']}'")
            ctx.commit()

    ctx.close()
