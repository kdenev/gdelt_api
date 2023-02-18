# Import packages
import time
import polars as pl
import pandas as pd
import json

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import Chrome

from datetime import datetime
from datetime import timedelta

DAYS = 1000
Start_Date = datetime.now().date() - timedelta(DAYS)


company = "apple"

base_url = "https://api.gdeltproject.org/api/v2/doc/doc?"

driver = Chrome()

scrape_df = pl.DataFrame()

for i in range(DAYS):

    news_start = (Start_Date + timedelta(i)).strftime("%Y%m%d")
    news_end = (Start_Date + timedelta(i+1)).strftime("%Y%m%d")
    
    q = f"query={company}" \
      "&maxrecords=250" \
      "&theme=ECON_STOCKMARKET " \
      f"&STARTDATETIME={news_start}000000" \
      f"&ENDDATETIME={news_end}000000" \
      "&format=JSON" 

    driver.get(base_url+q)
    # time.sleep(.2)
    json_dump = driver.find_element(By.TAG_NAME, 'body').get_attribute('innerText')
    try:
        page_df = pl.DataFrame(json.loads(json_dump, strict = False))
    except:
        page_df = pl.DataFrame()

    for row in page_df.rows():
        scrape_df = pl.concat([scrape_df, pl.DataFrame(row)])

output_df = scrape_df.filter(pl.col('language') == 'English').unique(subset='title')
output_df.write_csv('output_df.csv')