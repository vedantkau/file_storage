import sqlite3 as sq
import logging
import os

logger = logging.getLogger(__name__)


def _hash_row(concat_value: str) -> hash:
    """
    Returns hash of given string (concatenated value).
    """
    return hash(concat_value)


def lambda_handler(event, context):

    # connect to database and perform SCD Type 2 transformation
    # STAGING table holds scraped records and FINAL table holds all records from beginning
    logger.info(f"Performaing data transformations...")
    db = sq.connect('inshorts_db.sqlite')
    db.create_function("hash", 1, _hash_row)
    db.execute("""
    create table PRE_FINAL AS
    select fi.headline, fi.news_category, fi.news_url, fi.source_name, fi.source_url, fi.published_author, fi.published_datetime, fi.update_date from FINAL fi
    UNION
    select st.headline, st.news_category, st.news_url, st.source_name, st.source_url, st.published_author, st.published_datetime, min(st.update_date) from STAGING st
    """)
    db.commit()
    logger.info(f"STEP 1 completed, created PRE_FINAL")
    db.execute("delete from FINAL")
    logger.info(f"STEP 2 completed, truncated FINAL")
    db.execute("""
    INSERT into FINAL
    select headline, news_category, news_url, source_name, source_url, published_author, published_datetime, max(update_date)
    from PRE_FINAL
    group by headline, news_category, news_url, source_name, source_url, published_author, published_datetime 
    """)
    logger.info(f"STEP 3 completed, inserted records into FINAL")
    db.execute("drop table PRE_FINAL")
    logger.info(f"STEP 4 completed, dropped FINAL")
    db.execute("delete from STAGING")
    logger.info(f"STEP 5 completed, truncated STAGING")
    db.commit()
    db.close()

    logger.info(f"Finished data transformations")
