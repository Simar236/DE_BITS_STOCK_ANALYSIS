from setup.util_functions.load_file import insert_dataframe_postgres
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def process_stock_data(data):
    
    spark = SparkSession.builder \
        .appName("Stock Metrics Calculation") \
        .getOrCreate()

    df = spark.createDataFrame(data)

    df = df.withColumn('date', F.col('date').cast('date'))

    df = df.withColumn('year', F.year(F.col('date')))

    window_spec = Window.partitionBy('stock_symbol', 'year')

    df = df.withColumn('first_close', F.first('close').over(window_spec)) \
        .withColumn('last_close', F.last('close').over(window_spec)) \
        .withColumn('annual_cumulative_return', 
                    (F.col('last_close') - F.col('first_close')) / F.col('first_close')) \
        .withColumn('annual_end_performance', 
                    F.col('last_close') - F.col('first_close')) \
        .withColumn('last_traded_date', F.last('date').over(window_spec))

    aggregated_insights = df.groupBy('stock_symbol', 'year') \
        .agg(
            F.avg('close').alias('annual_avg_price'),
            F.stddev('close').alias('annual_volatility'),
            F.max('close').alias('annual_max_price'),
            F.min('close').alias('annual_min_price'),
            (F.max('close') - F.min('close')).alias('price_range'),
            F.avg('volume').alias('avg_volume'),
            F.count(F.when(F.col('close') > F.col('open'), True)).alias('days_price_up'),
            F.count(F.when(F.col('close') < F.col('open'), True)).alias('days_price_down'),
            F.first('annual_cumulative_return').alias('annual_cumulative_return'),
            F.first('annual_end_performance').alias('annual_end_performance'),
            F.first('last_traded_date').alias('last_traded_date')
        )

    # Show the results (for review before saving)
    # aggregated_insights.show(truncate=False)
    insert_dataframe_postgres(aggregated_insights.toPandas(), "stock_annual_insights")
    
    # # Ensure 'date' column is in correct format
    # df = df.withColumn('date', col('date').cast('date'))

    # # Add a column to indicate the 6-month period (1 for Jan-Jun, 2 for Jul-Dec)
    # df = df.withColumn('half', ((month(col('date')) - 1) / 6 + 1).cast('int'))

    # # Define window specification for stock_symbol and half
    # window_spec = Window.partitionBy('stock_symbol', 'half')

    # # Apply window functions to get first/last close prices for cumulative return
    # df = df.withColumn('first_close', first('close').over(window_spec)) \
    #     .withColumn('last_close', last('close').over(window_spec)) \
    #     .withColumn('six_month_cumulative_return', 
    #                 (col('last_close') - col('first_close')) / col('first_close')) \
    #     .withColumn('half_end_performance', 
    #                 col('last_close') - col('first_close')) \
    #     .withColumn('last_traded_date', last('date').over(window_spec))

    # # Perform the aggregation for the 6-month insights
    # aggregated_insights = df.groupBy('stock_symbol', 'half') \
    #     .agg(
    #         avg('close').alias('six_month_avg_price'),
    #         stddev('close').alias('six_month_volatility'),
    #         max('close').alias('six_month_max_price'),
    #         min('close').alias('six_month_min_price'),
    #         (max('close') - min('close')).alias('price_range'),
    #         avg('volume').alias('avg_volume'),
    #         count(when(col('close') > col('open'), True)).alias('days_price_up'),
    #         count(when(col('close') < col('open'), True)).alias('days_price_down'),
    #         first('six_month_cumulative_return').alias('six_month_cumulative_return'),
    #         first('half_end_performance').alias('half_end_performance'),
    #         first('last_traded_date').alias('last_traded_date')
    #     )

    # # Show the results (for review before saving)
    # aggregated_insights.show(truncate=False)
    spark.stop()
