# Import data packages
import polars as pl
import json
# Import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver import Chrome
# Import date packages
from datetime import datetime
from datetime import timedelta
import time

DAYS = 20
STARTDATE = datetime.now().date() - timedelta(DAYS)
KEYWORD = "tech"
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc?"

def get_articles(days:int, start_date:datetime, keyword:str, url:str, to_csv:bool = False) -> pl.DataFrame:
    """
    Function returns a polars DATAFRAME with ENGLISH news articles over a certain period,
    using the GDELT API.

    Given the limitation that the api, returning max 250 observations per query, this functions 
    creates a loop that go over a certain timeperiod. The timeriod is determined by the 
    STARTDATE and DAYS parameter. Starting from the STARTDATE it fetches the news related to the
    provided KEYWORD parameter one day at a time and appends to the DATAFRAME that it returns
    as main output.

    Optinal parameter TO_CSV will save a csv file with the output dataframe in the current
    working environment.  
    """
    # Init selenium
    driver = Chrome()

    # Placehodler dataframe
    scrape_df = pl.DataFrame()

    for i in range(days):

        # Assign iteration period    
        news_start = (start_date + timedelta(i)).strftime("%Y%m%d")
        news_end = (start_date + timedelta(i+1)).strftime("%Y%m%d")
        
        # Define executable query
        q = f"query={keyword}" \
        "&maxrecords=250" \
        "&theme=ECON_STOCKMARKET " \
        f"&STARTDATETIME={news_start}000000" \
        f"&ENDDATETIME={news_end}000000" \
        "&format=JSON" # Return the page result as json  

        # Send the query
        driver.get(url+q)
        # Add option deley if too quick
        time.sleep(.2)

        # Scrape the page/json contents
        json_dump = driver.find_element(By.TAG_NAME, 'body').get_attribute('innerText')
        # Cover the error of untranslatable characters
        try:
            page_df = pl.DataFrame(json.loads(json_dump, strict = False))
        except:
            page_df = pl.DataFrame()

        # Create dataframe with the scraped contetns
        for row in page_df.rows():
            scrape_df = pl.concat([scrape_df, pl.DataFrame(row)])

    # Currenty gets only the english articles
    # Remove the duplicate articels
    output_df = scrape_df.filter(pl.col('language') == 'English').unique(subset='title')

    # Ouput the csv file
    if to_csv:
        output_df.write_csv('output_df.csv')

    # Close selenium
    driver.close()

    return output_df

df = get_articles(DAYS, STARTDATE, KEYWORD, BASE_URL)

print(df)