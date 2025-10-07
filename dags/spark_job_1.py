from pyspark.sql import SparkSession, functions as F, Window

DB_CONFIG = {
    "host": "mysql",
    "port": "3306",
    "user": "root",
    "password": "1234",
    "database": "thesis"
}

def compute_indicators():
    # Tạo SparkSession
    spark = SparkSession.builder.appName("IndicatorsJob")\
        .config("spark.sql.shuffle.partitions", "8").getOrCreate()

    # Đọc dữ liệu kline
    url = f"jdbc:mysql://{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    df = spark.read.format("jdbc").options(
        url=url, driver="com.mysql.cj.jdbc.Driver",
        dbtable="kline_fact_1",
        user=DB_CONFIG["user"], password=DB_CONFIG["password"]
    ).load().select("symbol_id", "interval_id", "close_time", "close_price").cache()

    # Tạo cửa sổ tính toán
    w = Window.partitionBy("symbol_id", "interval_id").orderBy("close_time")

    # Trung bình cộng 14 điểm
    sma_df = df.withColumn("value", F.avg("close_price").over(w.rowsBetween(-13, 0)))\
               .withColumn("type_name", F.lit("SMA"))

    # Tính chênh lệch giá
    tmp = df.withColumn("diff", F.col("close_price") - F.lag("close_price").over(w))\
        .withColumn("gain", F.when(F.col("diff") > 0, F.col("diff")).otherwise(0.0))\
        .withColumn("loss", F.when(F.col("diff") < 0, -F.col("diff")).otherwise(0.0))
    avg_gain = F.avg("gain").over(w.rowsBetween(-13, 0))
    avg_loss = F.avg("loss").over(w.rowsBetween(-13, 0))
    rsi_df = tmp.withColumn("rs", avg_gain / avg_loss)\
        .withColumn("value", 100 - (100 / (1 + F.col("rs"))))\
        .withColumn("type_name", F.lit("RSI"))

    # Bollinger Bands
    bb_df = df.withColumn("mean", F.avg("close_price").over(w.rowsBetween(-13, 0)))\
        .withColumn("stddev", F.stddev("close_price").over(w.rowsBetween(-13, 0)))
    bb_up_df   = bb_df.withColumn("value", F.col("mean") + 2*F.col("stddev")).withColumn("type_name", F.lit("BB_UP"))
    bb_down_df = bb_df.withColumn("value", F.col("mean") - 2*F.col("stddev")).withColumn("type_name", F.lit("BB_DOWN"))

    # Gộp tất cả indicators
    select_cols = ["symbol_id", "interval_id", "type_name", "value", F.col("close_time").alias("timestamp")]

    indicators_df = (sma_df.select(*select_cols)
        .unionByName(rsi_df.select(*select_cols))
        .unionByName(bb_up_df.select(*select_cols))
        .unionByName(bb_down_df.select(*select_cols))
        .filter(F.col("value").isNotNull()))

    # Đọc dim_indicator_type -> ánh xạ name -> id
    dim_type_df = spark.read.format("jdbc").options(
        url=url, driver="com.mysql.cj.jdbc.Driver",
        dbtable="dim_indicator_type_1",
        user=DB_CONFIG["user"], password=DB_CONFIG["password"]
    ).load()

    # Join để lấy type_id
    indicators_df = indicators_df.join(
        dim_type_df,
        indicators_df.type_name == dim_type_df.type_name,
        "inner"
    ).select(
        "symbol_id", "interval_id",
        F.col("type_id"), "value", "timestamp"
    )

    # Lọc bản ghi đã tồn tại
    existing_df = spark.read.format("jdbc").options(
        url=url, driver="com.mysql.cj.jdbc.Driver",
        dbtable="indicator_fact_1",
        user=DB_CONFIG["user"], password=DB_CONFIG["password"]
    ).load().select("symbol_id", "interval_id", "type_id", "timestamp")

    # Giữ lại những record chưa tồn tại
    indicators_df = indicators_df.join(
        existing_df,
        on=["symbol_id", "interval_id", "type_id", "timestamp"],
        how="left_anti"
    )

    # Ghi kết quả
    if not indicators_df.rdd.isEmpty():
        indicators_df.write.format("jdbc").options(
            url=url, driver="com.mysql.cj.jdbc.Driver",
            dbtable="indicator_fact_1",
            user=DB_CONFIG["user"], password=DB_CONFIG["password"]
        ).mode("append").save()
        print("[DONE] Written new indicators.")
    else:
        print("[DONE] No new indicators.")
    spark.stop()

if __name__ == "__main__":
    compute_indicators()
