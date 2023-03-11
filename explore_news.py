# Data management
import polars as pl
import pandas as pd
# Sentiment
from nltk.sentiment.vader import SentimentIntensityAnalyzer
# Prices
import yfinance as yf
# Plotting
from bokeh.plotting import figure, gridplot
from bokeh.io import show
# from bokeh.sampledata.stocks import AAPL
from bokeh.models import (ColumnDataSource, CDSView, GroupFilter, 
                          HoverTool, PanTool, ZoomInTool, ZoomOutTool, 
                          ResetTool, DatetimeTickFormatter, CustomJS,
                          NumeralTickFormatter, CrosshairTool, TapTool,
                          MultiLine, Range1d, Label)
from datetime import timedelta
from sklearn.metrics import confusion_matrix, accuracy_score

def get_stock_info(ticker, days: int):
# Download the stock info
    ticker_info = yf.Ticker(ticker)
    period = str(days) + 'd'
    prices = pl.DataFrame(ticker_info.history(period).reset_index())

    # Adjust the downloaded df
    prices_adj = (
        prices
        .select(['Date', 'Close', 'Volume'])
        .with_columns(
                        [
                            pl.col('Date')
                            .dt.strftime(fmt="%Y-%m-%d")
                            .str.strptime(pl.Date, fmt="%Y-%m-%d", strict=False)
                            , pl.col('Close').round(2)
                        ]
                    )

    )

    return prices_adj


sid = SentimentIntensityAnalyzer()

df = (pl.read_csv('gdelt_api\crypto_news_df.csv')
      .with_columns([
                    
                    pl.col('seendate') 
                    .str.strptime(pl.Datetime, fmt="%Y%m%dT%H%M%SZ")
                    .dt.strftime("%Y-%m-%d")
                    .str.strptime(pl.Date, fmt="%Y-%m-%d")
                    .alias('date')

                    , pl.col('title').apply(lambda x: sid.polarity_scores(x)['compound']).alias('sentiment')

                ])
        .groupby(['date','domain'])
        .agg([
            pl.count().alias('count')
            , pl.sum('sentiment').alias('daily_sentiment')
        ])
        .sort('date')
            )

# treshold = len(df['date'].unique())*.7
news_outlets = (df
                .groupby('domain')
                .agg([
                        pl.max('date').dt.year().alias('max_year')
                        , pl.count().alias('count')

                    ])
                .filter(pl.col('max_year') == 2023)
                .sort('count',reverse=True)
                .head(10)
                .select('domain')
                .to_series()
                )

df_filtered  = (df
                .filter(pl.col('domain').is_in(news_outlets))
                .with_column(
                    pl.when(pl.col('daily_sentiment')>0)
                    .then('green')
                    .otherwise('red')
                    .alias('sentiment_color')
                )
            )
prices = (get_stock_info('BNB-USD', 1000)
          .with_columns([
            pl.col('Close').shift(-1).alias('lead_price')
            , ((pl.col('Close')/pl.col('Close').shift(1) -1) > 0).alias('up_down')
          ])
          .with_columns([
    
            ((pl.col('lead_price')/pl.col('Close') -1) > 0).alias('lead_up_down')
            , pl.when((pl.col('lead_price')/pl.col('Close') -1) > 0).then('green').otherwise('red').alias('day_color')
        ])
          .drop_nulls()
        )


df_plot = df_filtered.join(
            prices
            , left_on='date'
            , right_on='Date'
            , how='inner'
    ).sort('date').to_pandas()


############## Ploting ##################

# df_plot.Date = pd.to_datetime(df_plot['date'])

glyph_list = list()

for i in news_outlets:
    
    source = ColumnDataSource(df_plot.loc[df_plot['domain'] == i].iloc[:,:])

    f = figure(x_axis_type = 'datetime'
                , height=800
                , width=1000
                , title = i)

    f.line(
        x= 'date'
        , y='lead_price'
        , source = source
        , width = 2
    )

    f.vbar(
        x = 'date'
        , top = 'lead_price'
        , color = 'sentiment_color'
        , source = source
        , width = timedelta(1)
        , alpha = .2
        , line_alpha = 0
    )

    f.circle(
        x = 'date'
        , y = 'lead_price'
        , color = 'day_color'
        , source = source
        , size = 10
        , alpha = .2
    )

    tn, fp, fn, tp = confusion_matrix(source.data['lead_up_down'], source.data['daily_sentiment']>0).ravel()
    accuracy = round(accuracy_score(source.data['lead_up_down'], source.data['daily_sentiment']>0),2)

    citation = Label(x=70, y=600, x_units='screen', y_units='screen'
                 ,text=f'TN: {tn} FP: {fp}\nFN: {fn} TP: {tp}\nAccuracy: {accuracy}', border_line_color='black', border_line_alpha=1.0,
                 background_fill_color='white', background_fill_alpha=1.0)
    
    f.add_layout(citation)

    glyph_list.append([f])
    # # map dataframe indices to date strings and use as label overrides
    # f.xaxis.major_label_overrides = {
    #     i: date.strftime('%b %d') for i, date in enumerate(pd.to_datetime(df["date"]))
    # }
    # f.y_range = Range1d(120,max(df['Close'])+5)


grid = gridplot(glyph_list)

show(grid)

# Combined plot of best performers

# combined_sent = df_plot[['date', 'daily_sentiment', 'domain', 'up_down']].loc[df_plot['domain'].isin(['cointelegraph.com', 'fxstreet.com', 'forextv.com', 'bnnbloomberg.ca'])]
sent_pivot = (df_filtered
              .pivot(index='date', columns=['domain'], values='daily_sentiment')
              .fill_null(0)
              .sort('date')
              .join(prices[['Date', 'Close', 'up_down', 'lead_price', 'lead_up_down']]
                    ,left_on='date'
                    ,right_on='Date'
                    ,how='inner'
                )
              .to_pandas())

sent_pivot.to_csv('btc_news_separated_model\\filtered_news_bnb_sent.csv', index=False)
