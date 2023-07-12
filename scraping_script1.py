import os
import json
import logging
import sqlite3 as sq
import datetime as dt
from bs4 import BeautifulSoup as bs
import requests

logger = logging.getLogger(__name__)


def _scrape_category(news_category: str) -> list[dict]:
    """
    Scrape the news information of given news category.

    Parameters:
    news_category (str): Inshorts news category

    Returns:
    list[dict]: List of dictionaries of news information. Dictionary keys: [headline, news_category, news_link, source_url, source_name, published_author, published_datetime]
    """
    URL = "https://www.inshorts.com/en/ajax/more_news"
    payload = json.dumps({"category": news_category, })
    HEADERS = {
        'origin': 'https://www.inshorts.com',
        'Content-Type': 'application/json'
    }
    NEWS_SCRAPE_COUNT = 5
    # collect response from inshorts
    logger.info(f"Category: {news_category} - request calls started ...")
    full_response = ""
    for _ in range(NEWS_SCRAPE_COUNT):
        response = requests.request("POST", URL, headers=HEADERS, data=payload)
        resp_json = response.json()
        payload = json.dumps(
            {"category": news_category, "news_offset": resp_json.get("min_news_id")})
        full_response += resp_json.get("html")
    logger.info(f"Category: {news_category} - request calls finished")

    # create data dictionary of news information
    logger.info(f"Category: {news_category} - extracting data ...")
    data_dict = []
    first_pass = bs(full_response, 'html.parser').find_all("div", class_="news-card")
    for ele in first_pass:
        temp_dict = {}
        try:
            temp_dict["headline"] = ele.find("span", itemprop="headline").text
            temp_dict["news_category"] = news_category
            temp_dict["news_url"] = ele.find('span', itemprop="mainEntityOfPage")["itemid"]
            try:
                news_source = ele.find("a", class_="source")
                temp_dict["source_url"] = news_source["href"]
                temp_dict["source_name"] = news_source.text
            except:
                temp_dict["source_name"] = ""
                temp_dict["source_url"] = ""

            second_pass_pub = ele.find("div", class_="news-card-author-time-in-title")
            temp_dict["published_author"] = second_pass_pub.find("span", class_="author").text
            temp_dict["published_datetime"] = second_pass_pub.find("span", itemprop="datePublished")["content"]

            data_dict.append(temp_dict)

        except Exception as e:
            logger.error("Failed to process element: "+str(ele))
            logger.error(e)
    logger.info(f"Category: {news_category} - extracting data finished")
    
    return data_dict


def _persist_data(data_dict: list[dict]):

    # connect to database and create tables if not exists
    # STAGING table holds scraped records and FINAL table holds all records from beginning
    logger.info(f"Performaing data operations...")
    db = sq.connect("inshorts_db.sqlite")
    db.execute("""
    CREATE TABLE IF NOT EXISTS STAGING
    (headline VARCHAR(200),
    news_category VARCHAR(15),
    news_url VARCHAR(250),
    source_name VARCHAR(30),
    source_url VARCHAR(250),
    published_author VARCHAR(50),
    published_datetime VARCHAR(25),
    update_date date)
    """)
    db.execute("""
    CREATE TABLE IF NOT EXISTS FINAL
    (headline VARCHAR(200),
    news_category VARCHAR(15),
    news_url VARCHAR(250),
    source_name VARCHAR(30),
    source_url VARCHAR(250),
    published_author VARCHAR(50),
    published_datetime VARCHAR(25),
    update_date date)
    """)
    # insert scraped records into STAGING table
    try:
        db.executemany("""
        INSERT INTO STAGING(headline, news_category, news_url, source_name, source_url, published_author, 
        published_datetime, update_date) 
        VALUES(:headline, :news_category, :news_url, :source_name, :source_url, :published_author, :published_datetime, {0})
        """.format(f'\'{dt.date.today().strftime("%Y-%m-%d")}\''), data_dict)
    except:
        logger.error(f"Data loading to STAGING table failed")
        raise

    db.commit()
    db.close()
    logger.info(f"Data operations finished")


def lambda_handler(event, context):

    categories = ["national", "business", "sports", "world", "politics", "technology", "startup", "entertainment", "miscellaneous", "hatke", "science", "automobile"]

    # create a full dictionary of news information from all categories
    final_data = []
    for cat in categories:
        logger.info(f"Calling scrape function for category {cat}")
        print("Hiii")
        final_data.extend(_scrape_category(cat))
        logger.info(f"Function call finished for category {cat}")

    _persist_data(final_data)
    logger.info(f"Script finished")

