import datetime
import logging
import pandas as pd
import requests
import azure.functions as func
from sqlalchemy import create_engine


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = (
        datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    )

    if mytimer.past_due:
        logging.info("The timer is past due!")

    dfs = []
    headers = {"app-language": "he"}
    city_mapping = {
        "yavne": "lat=31.861934040036914&lon=34.73880889349573",
        "netanya": "lat=32.30275178459601&lon=34.8626069423332",
        "pardes-hanna": "lat=32.458223691314956&lon=34.92813930894266",
        "ashdod": "lat=31.8226123073281&lon=34.656207787820875",
        "beer-sheva": "lat=31.25049530861928&lon=34.79046992070812",
        "herzliya": "lat=32.168344232767616&lon=34.83246849366997",
        "hasharon": "lat=32.17538379893091&lon=34.89257605007606",
        "haifa": "lat=32.79952296612683&lon=34.98459479897443",
        "jerusalem": "lat=31.771914195076675&lon=35.20696303876315",
        "modiin": "lat=31.89980146656727&lon=35.00538115516542",
        "ness-ziona---rehovot": "lat=31.902428004772347&lon=34.80746574626252",
        "petah-tikva": "lat=32.08889061990374&lon=34.886085765343694",
        "rishon-lezion": "lat=31.997324147971867&lon=34.780000379407966",
        "tel-aviv": "lat=32.087236876497585&lon=34.78698525756491",
    }

    for city in city_mapping:
        r = requests.get(
            f"https://restaurant-api.wolt.com/v1/pages/restaurants?{city_mapping[city]}",
            headers=headers,
        )
        restaurants = pd.DataFrame.from_records(r.json()["sections"][1]["items"])

        for i, row in restaurants.iterrows():
            try:
                rest = row["track_id"].replace("venue-", "")
                title = row["title"]
                url = f"https://restaurant-api.wolt.com/v4/venues/slug/{rest}/menu?unit_prices=true&show_weighted_items=true&language=he"
                response = requests.get(url).json()
                df = pd.DataFrame.from_records(response["items"])
                df["title"] = title
                df["url"] = "https://wolt.com/he/isr/tel-aviv/restaurant/" + rest
                df["city"] = city
                dfs.append(df)
            except:
                continue

    master = pd.concat(dfs)
    master["price"] = master["baseprice"] / 100

    engine = create_engine("sqlite:///items.db", echo=True)

    df = master[["city", "title", "name", "description", "price", "image", "url"]]
    df.to_sql("items", if_exists="replace", con=engine)

    logging.info("Python timer trigger function ran at %s", utc_timestamp)
