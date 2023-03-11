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

sid = SentimentIntensityAnalyzer()

df = (pl.read_csv('gdelt_api\crypto_news_hours_df.csv')
      .with_columns([

                    pl.col('seendate')
                    .str.strptime(pl.Datetime, fmt="%Y%m%dT%H%M%SZ")
                    .dt.strftime("%Y-%m-%d %H")
                    .str.strptime(pl.Datetime, fmt="%Y-%m-%d %H")
                    .alias('date'), pl.col('title').apply(lambda x: sid.polarity_scores(x)['compound']).alias('sentiment')

                    ])
        .groupby(['date', 'domain'])
        .agg([
            pl.count().alias('count'), pl.sum('sentiment').alias('daily_sentiment')
        ])
      .sort('date')
      )

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

df_filtered =(df
    .filter((pl.col('domain').is_in(news_outlets)) & (pl.col('date').dt.hour() < 12))
    .with_column(pl.col('date').dt.strftime("%Y-%m-%d").str.strptime(pl.Date, fmt="%Y-%m-%d").alias('join_date'))
    .groupby('join_date')
    .agg([
        pl.sum('daily_sentiment').alias('sentiment')
    ])
    .with_column(pl.when(pl.col('sentiment') > 0)
                 .then('green')
                 .otherwise('red')
                 .alias('sentiment_color')))

price_days = (pl.read_parquet('binance_api\\btc_future_price.parquet')
              .with_columns([
                            (pl.col('close') > pl.col('close').shift(1)).alias('up_down')
                            ,pl.when(pl.col('close') > pl.col('close').shift(1)).then('green').otherwise('red').alias('day_color')
                        ])
              .select(['close_time', 'close', 'up_down', 'day_color'])
              .drop_nulls()
              )
prices_hours = (pl.read_parquet('btc_hour_price_model\\btc_future_price_hours.parquet')
            .with_columns([
                    pl.col('open_time').dt.strftime("%Y-%m-%d").str.strptime(pl.Date, fmt="%Y-%m-%d").alias('date')
                    , pl.col('close').alias('close_hour')
                ])
            .drop('close')
            .join(price_days, left_on='date', right_on='close_time', how='inner')
            .filter(pl.col('open_time').dt.hour() == 11)
            .with_column(pl.col('open').alias('open_11am'))
            # .with_columns([
            #         pl.col('close_hour').max().over('date').alias('last_price')
            #     ])
            .select(['date', 'open_11am', 'close', 'up_down', 'day_color'])
          .drop_nulls()
        )

df_plot = df_filtered.join(
    prices_hours, left_on='join_date', right_on='date', how='left'
).drop_nulls().to_pandas()


############## Ploting ##################

# df_plot.Date = pd.to_datetime(df_plot['date'])

glyph_list = list()

for i in news_outlets:

    source = ColumnDataSource(df_plot.loc[df_plot['domain'] == i].iloc[:,:])

    f = figure(x_axis_type='datetime', height=800, width=1000, title=i)

    f.line(
        x='date', y='close', source=source, width=2
    )

    f.vbar(
        x='date', top='close', color='sentiment_color', source=source, width=timedelta(hours=1), alpha=.2, line_alpha=0
    )

    f.circle(
        x='date', y='close', color='day_color', source=source, size=10, alpha=.2
    )

    tn, fp, fn, tp = confusion_matrix(
        source.data['up_down'], source.data['daily_sentiment'] > 0).ravel()
    accuracy = round(accuracy_score(
        source.data['up_down'], source.data['daily_sentiment'] > 0), 2)

    citation = Label(x=70, y=600, x_units='screen', y_units='screen', text=f'TN: {tn} FP: {fp}\nFN: {fn} TP: {tp}\nAccuracy: {accuracy}', border_line_color='black', border_line_alpha=1.0,
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

# sent_pivot = df_filtered.to_pandas().pivot(
#     index='date', columns=['domain'], values='daily_sentiment').reset_index()
# sent_pivot.fillna(0, inplace=True)

sent = df_filtered.select(['join_date', 'sentiment']).sort('join_date').to_pandas()

df = sent.merge(prices_hours
                      .to_pandas()[['date', 'open_11am', 'close', 'up_down']], left_on='join_date', right_on='date', how='inner'
                      )

df.to_parquet('btc_news_separated_model\\filtered_hour_news_sent.parquet')

