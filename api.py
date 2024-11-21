from flask import Flask, jsonify
from pyspark.sql import SparkSession


app = Flask(__name__)


spark = SparkSession.builder.appName("DataLakeAPI").getOrCreate()


transactions_df = spark.read.parquet("./processed_data/transactions/")
web_logs_df = spark.read.text("./processed_data/web_logs/")
social_media_df = spark.read.json("./processed_data/social_media/")


def df_to_dict(df):
    return [row.asDict() for row in df.collect()]


@app.route('/api/transactions', methods=['GET'])
def get_transactions():
    data = df_to_dict(transactions_df)
    return jsonify(data)


@app.route('/api/web_logs', methods=['GET'])
def get_web_logs():
    data = df_to_dict(web_logs_df)
    return jsonify(data)


@app.route('/api/social_media', methods=['GET'])
def get_social_media():
    data = df_to_dict(social_media_df)
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
