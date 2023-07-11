<img align="right" alt="gdelt" width="900" height="400"
    src="https://blog.gdeltproject.org/wp-content/uploads/2015-gdelt-2.png">
    
<h1 align="center">How to query GDELT API using Selenium</h1>

<p align="center">Use selenium to create english news dataset</p>
<p>Using the function get_articles users can create news datasets by providing a keyword. Currently it only fetches articles in English related to the stock market. Does not allow for duplicate articles in the resulting dataframe.</p>

<p>Opportunities for further development - support for multiple keywords and article themes(other than the stock market).</p>

<p>The function uses 4(5) parameters: </p>
<li>DAYS: Integer</li>
<li>START_DATE: Datetime</li>
<li>KEYWORD: String</li>
<li>URL: String</li>
<li>*optional TO_CSV: Boolean</li>
<p></p>
<p>Define main parameter:</p>

```python
HOURS = 1*24
STARTDATE = datetime.strptime(str(datetime.now())[:13], '%Y-%m-%d %H') - timedelta(hours=HOURS)
KEYWORDS = ['jerome', 'powell', 'fed']
BASE_URL = "https://api.gdeltproject.org/api/v2/doc/doc?"
```

<p>Compile function:</p>

```python
def get_articles(hours:int, start_date:datetime, keywords: List[str], url:str, to_csv:bool = False) -> pl.DataFrame:
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

    for i in range(hours):

        # Assign iteration period    
        news_start = (start_date + timedelta(hours=i)).strftime("%Y%m%d%H")
        news_end = (start_date + timedelta(hours=i+1)).strftime("%Y%m%d%H")
        
        #print(f"Start:{news_start}, End:{news_end}")

        # Define executable query
        q = f"query=({to_or(keywords)})" \
        "&maxrecords=250" \
        "&theme=ECON_STOCKMARKET " \
        f"&STARTDATETIME={news_start}0000" \
        f"&ENDDATETIME={news_end}0000" \
        "&format=JSON" # Return the page result as json  

        # Send the query
        driver.get(url+q)
        # Add option deley if too quick
        time.sleep(.1)

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

```

<p>Call the function:</p>

```python
df = get_articles(HOURS, STARTDATE, KEYWORDS, BASE_URL, True)
```

<p>Used Libraries:</p>

```python
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
```
