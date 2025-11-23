"""
Stream transformations and enrichment module
"""
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.functions import col, when, lit, avg


def transform_weather(weather_clean: DataFrame) -> DataFrame:
    """Apply weather-specific transformations"""
    weather_transformed = weather_clean \
        .withColumn(
            "feels_like_temp",
            col("temperature_celsius") - 
            (0.55 * (1 - 0.16 * col("wind_speed")) * (col("temperature_celsius") - 14.1))
        ) \
        .withColumn(
            "wind_category",
            when(col("wind_speed") < 1, lit("Calm"))
            .when(col("wind_speed") < 2, lit("Light"))
            .when(col("wind_speed") < 5, lit("Gentle"))
            .when(col("wind_speed") < 8, lit("Moderate"))
            .when(col("wind_speed") < 11, lit("Fresh"))
            .when(col("wind_speed") < 14, lit("Strong"))
            .when(col("wind_speed") < 17, lit("Gale"))
            .otherwise(lit("Storm"))
        ) \
        .withColumn(
            "weather_anomaly",
            when(col("wind_speed") >= 14, lit("High wind anomaly"))
            .when(col("precipitation") > 50, lit("Heavy precipitation anomaly"))
            .when(
                (col("temperature_celsius") > 35) | (col("temperature_celsius") < -15),
                lit("Extreme temperature anomaly")
            )
            .otherwise(lit("Normal"))
        )
    
    print("Weather transformations: feels_like, wind_category, anomaly_detection")
    return weather_transformed


def transform_air_quality(air_quality_clean: DataFrame) -> DataFrame:
    """Apply air quality-specific transformations"""
    air_quality_transformed = air_quality_clean \
        .withColumn(
            "aqi_category",
            when(col("parameter_normalized") == "pm25", 
                when(col("value_standard_unit") <= 12, lit("Good"))
                .when(col("value_standard_unit") <= 35.4, lit("Moderate"))
                .when(col("value_standard_unit") <= 55.4, lit("Unhealthy for Sensitive Groups"))
                .when(col("value_standard_unit") <= 150.4, lit("Unhealthy"))
                .otherwise(lit("Very Unhealthy"))
            )
            .when(col("parameter_normalized") == "pm10",
                when(col("value_standard_unit") <= 50, lit("Good"))
                .when(col("value_standard_unit") <= 150, lit("Moderate"))
                .when(col("value_standard_unit") <= 250, lit("Unhealthy for Sensitive Groups"))
                .when(col("value_standard_unit") <= 350, lit("Unhealthy"))
                .otherwise(lit("Very Unhealthy"))
            )
            .when(col("parameter_normalized") == "o3",
                when(col("value_standard_unit") <= 54, lit("Good"))
                .when(col("value_standard_unit") <= 70, lit("Moderate"))
                .when(col("value_standard_unit") <= 85, lit("Unhealthy for Sensitive Groups"))
                .when(col("value_standard_unit") <= 105, lit("Unhealthy"))
                .otherwise(lit("Very Unhealthy"))
            )
            .otherwise(lit("Not Classified"))
        ) \
        .withColumn(
            "pollution_spike",
            when(col("value_standard_unit") > 100, lit(True)).otherwise(lit(False))
        ) \
        .withColumn(
            "health_risk_level",
            when(col("aqi_category").isin("Good"), lit(0))
            .when(col("aqi_category").isin("Moderate"), lit(1))
            .when(col("aqi_category").isin("Unhealthy for Sensitive Groups"), lit(2))
            .when(col("aqi_category").isin("Unhealthy"), lit(3))
            .when(col("aqi_category").isin("Very Unhealthy"), lit(4))
            .otherwise(lit(-1))
        )
    
    print("Air quality transformations: AQI, health classification, spike detection")
    return air_quality_transformed


def transform_social(social_clean: DataFrame) -> DataFrame:
    """Apply social post transformations"""
    social_transformed = social_clean \
        .withColumn("engagement_score", F.col("upvotes") * 0.8) \
        .withColumn("text_length", F.length(col("body_normalized"))) \
        .withColumn("text_for_bert", col("body_normalized"))
    
    print("Social posts transformations: engagement scoring, text prep")
    return social_transformed
