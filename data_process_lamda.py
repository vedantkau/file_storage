import sqlite3 as sq


def _hash_row(concat_value):
    return hash(concat_value)


db = sq.connect("inshorts_scrape.sqlite")
db.create_function("hash", 1, _hash_row)
db.execute("""
create table PRE_FINAL AS
select *, dense_rank() over (PARTITION by key_hash, change_hash order by update_date asc) as row_rank from 
(select *, hash(headline || news_category || source_name || source_url || published_author || published_datetime) as change_hash from STAGING
UNION
select *, hash(headline || news_category || source_name || source_url || published_author || published_datetime) as change_hash from FINAL)
""")
db.execute("delete from FINAL")
db.execute("""
INSERT into FINAL
select headline, news_category, news_url, source_name, source_url, published_author, published_datetime, key_hash, update_date
from PRE_FINAL where row_rank = 1
""")
db.execute("drop table PRE_FINAL")
db.commit()
db.close()