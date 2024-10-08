import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    count,
    current_date,
    datediff,
    desc,
    max,
    min,
    round,
    size,
    split,
    struct,
    to_date,
    to_json,
    udf,
)
from pyspark.sql.types import FloatType, IntegerType, StringType, StructField, StructType


class Processor:
    THRESHOLD = 0.2
    YEARS = 3
    SCHEMA = StructType(
        [
            StructField("restaurantId", IntegerType(), nullable=False),
            StructField("reviewId", IntegerType(), nullable=False),
            StructField("text", StringType(), nullable=False),
            StructField("rating", FloatType(), nullable=False),
            StructField("publishedAt", StringType(), nullable=False),
        ]
    )

    def __init__(self, spark):
        self.spark = spark
        self._df = None
        self._inappropriate_words = None
        self._aggregated_df = None

    def read_and_clean_reviews(self, reviews_path):
        self._df = self.spark.read.option("mode", "PERMISSIVE").schema(self.SCHEMA).json(reviews_path)
        # Deduplicate and only return the latest record
        self._df = self._df.orderBy(desc("publishedAt")).dropDuplicates(["restaurantId", "reviewId"])
        # Delete bad ratings reviews
        self._df = self._df.filter((self._df["rating"] >= 1) & (self._df["rating"] <= 10))
        return self

    def read_inappropriate_words(self, inappropriate_words_path):
        with open(inappropriate_words_path, "r") as file:
            self._inappropriate_words = [word.strip().lower() for word in file]
        return self

    @staticmethod
    def write_into_jsonl(df, file_path):
        json_df = df.select(to_json(struct([df[x] for x in df.columns])).alias("json"))
        json_strings = json_df.rdd.map(lambda x: x.json).collect()
        with open(file_path, "w") as f:
            for json_string in json_strings:
                f.write(json_string + "\n")

    def filter_inappropriate_words(self):
        inappropriate_words = self._inappropriate_words
        threshold = self.THRESHOLD

        def filtering(text, length):
            pattern = r"\b(" + "|".join(re.escape(word) for word in inappropriate_words) + r")\b"
            processed_text, count = re.subn(pattern, "****", text)
            if count / length > threshold:
                return None  # Discard the review
            else:
                return processed_text

        filter_review_udf = udf(filtering, StringType())

        # Apply text processing
        self._df = self._df.withColumn("length", size(split(self._df["text"], " ")))
        self._df = self._df.withColumn("processed_text", filter_review_udf(col("text"), col("length")))
        self._df = self._df.filter(col("processed_text").isNotNull())
        return self

    def filter_old_reviews(self):
        # Convert published_at to date
        self._df = self._df.withColumn("publishedDate", to_date(col("publishedAt")))
        self._df = self._df.withColumn("review_age", datediff(current_date(), self._df["publishedDate"]))
        # Filter out reviews older than 3 years
        self._df = self._df.filter(self._df.review_age < self.YEARS * 365)
        return self

    def save_processed_reviews(self, output_path):
        # Select and rename columns for output
        self.write_into_jsonl(
            self._df.select(
                col("restaurantId"),
                col("reviewId"),
                col("processed_text").alias("text"),
                col("rating"),
                col("publishedAt"),
            ),
            output_path,
        )

    def aggregate(self, aggregation_path):
        # Select and rename columns for aggregation
        self.write_into_jsonl(
            self._df.filter(self._df["restaurantId"].isNotNull())
            .groupBy("restaurantId")
            .agg(
                count("reviewId").alias("reviewCount"),
                round(avg("rating"), 2).alias("averageRating"),
                round(avg("length"), 2).alias("averageReviewLength"),
                struct(
                    min("review_age").alias("newestReview"),
                    max("review_age").alias("oldestReview"),
                    round(avg("review_age"), 2).alias("averageReviewAge"),
                ).alias("reviewAge"),
            ),
            aggregation_path,
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input", required=True, help="Local filesystem path to the JSONL file containing the reviews"
    )
    parser.add_argument(
        "-iw",
        "--inappropriate_words",
        required=True,
        help="Local filesystem path to the new-line delimited text file containing the words to filter out",
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="Local filesystem path to a single JSONL file to write the successfully processed reviews to",
    )
    parser.add_argument(
        "-agg",
        "--aggregation",
        required=True,
        help="Local filesystem path to a single JSONL file to write the aggregations to",
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("ReviewProcessing").getOrCreate()
    review_processor = Processor(spark).read_and_clean_reviews(args.input)
    review_processor.read_inappropriate_words(args.inappropriate_words)
    review_processor.filter_inappropriate_words().filter_old_reviews()
    review_processor.save_processed_reviews(args.output)
    review_processor.aggregate(args.aggregation)
    spark.stop()
