from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.types as tp
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import csv
import json
from datetime import datetime
from elasticsearch import Elasticsearch
from pyspark.sql.functions import from_json
from pyspark.sql.functions import trim

kafkaServer = "kafka:39092"
topic = "music"

SparkConf = SparkConf().set("es.nodes", "elasticsearch").set("es.port", "9200")
sc = SparkContext(conf=SparkConf, appName="MusicRegression")
spark = SparkSession(sc)
sc.setLogLevel("ERROR")

schema = tp.StructType([
    tp.StructField(name='Id', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='DateTime', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='Title', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='Artist', dataType=tp.StringType(), nullable=True),
    tp.StructField(name='Likes', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Comments', dataType=tp.IntegerType(), nullable=True),
    tp.StructField(name='Streams', dataType=tp.LongType(), nullable=True),
    tp.StructField(name='Position', dataType=tp.IntegerType(), nullable=True),
])

def train_regression_model():
    elastic_host = "http://elasticsearch:9200"
    elastic_index = "ytmusic"

    print("Reading training set...")
    training_set = spark.read.csv('/app/data.csv', header=True, schema=schema, sep=',')
    print("Done.")

    # Indicizzazione della colonna 'Title'
    indexer = StringIndexer(inputCol='Title', outputCol='Title_indexed')
    indexed = indexer.fit(training_set).transform(training_set)

    assembler = VectorAssembler(inputCols=['Title_indexed', 'Streams'], outputCol='features')
    output = assembler.transform(indexed)
    output.select('features', 'Position').show()

    #final_data = output.select('Title', 'Artist' ,'Title_indexed', 'features', 'Position')  
    final_data = output.select('DateTime', 'Title', 'Artist', 'Likes', 'Streams', 'Title_indexed', 'features', 'Position')

    # Raccolta degli ID univoci dei titoli
    unique_title_ids = final_data.select('Title_indexed').distinct().rdd.flatMap(lambda x: x).collect()

    regression_models = {}
    predictions = {}

    print("Training model...")
    # Esecuzione della regressione lineare e previsione per ogni ID del titolo unico
    for title_id in unique_title_ids:
        id_value = title_id
        print("Execute linear regression and prediction for Title_indexed:", id_value)

        # Filtraggio dei dati per l'ID del titolo corrente
        filtered_data = final_data.filter(final_data.Title_indexed == id_value).filter(final_data.Position.isNotNull())

        train_data, test_data = filtered_data.randomSplit([1.0, 0.0])

        lr = LinearRegression(featuresCol='features', labelCol='Position')

        lr_model = lr.fit(train_data)

        result = lr_model.evaluate(train_data)

        regression_models[id_value] = lr_model
        predictions[id_value] = result

        r2 = result.r2
        if r2 is not None and not (r2 != r2):
            print("R-Squared error:", r2)
        else:
            print("R-Squared error: NaN")

        # Selezione delle feature per la previsione
        unlabeled_data = filtered_data.select('features')

        # Esecuzione della previsione utilizzando il modello di regressione memorizzato
        prediction = lr_model.transform(unlabeled_data)

        # # Visualizzazione delle previsioni
        prediction.show()

        # Invio dei dati a Elasticsearch
        title = filtered_data.select('Title').first()[0]  
        position = filtered_data.select('Position').first()[0]
        prediction_value = prediction.select('prediction').first()[0]
        artist = filtered_data.select('Artist').first()[0]
        likes = filtered_data.select('Likes').first()[0]
        streams = filtered_data.select('Streams').first()[0]
        datetime = filtered_data.select('DateTime').first()[0]

        doc = {
            'title': title,
            'position': position,
            'prediction': prediction_value,
            'artist': artist,
            'likes': likes,
            'streams': streams,
            'datetime': datetime

        }

        es = Elasticsearch(elastic_host)
        es.index(index=elastic_index, body=doc, ignore=400)
    print("Done.")


def write_to_csv(record):
    converted_dict = json.loads(record["value"])

    with open('/app/data.csv', 'a', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)

        # Append new data row
        title = converted_dict['title'].replace(',', '-')
        artist = converted_dict['artist'].replace(',', '-')
        new_row = [
            converted_dict['id'],
            datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            title,  # Replace comma with dash in title
            artist,  # Replace comma with dash in artist
            converted_dict['likes'],
            converted_dict['comments'],
            converted_dict['streams'],
            converted_dict['position']
        ]
        writer.writerow(new_row)

    # Read the content of the CSV file
    with open('/app/data.csv', 'r', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        rows = list(reader)

    # Find rows with the same position but different title
    position_title_map = {}
    for row in rows:
        row_position = row[7]  # Assuming position is at index 7
        row_title = row[2]  # Assuming title is at index 2
        row_datetime_str = row[1]  # Assuming datetime is at index 1

        try:
            row_datetime = datetime.strptime(row_datetime_str, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            continue

        if row_position in position_title_map:
            existing_title, existing_datetime = position_title_map[row_position]

            # Check if the current row has a different title
            if row_title != existing_title:
                # Check if the current row's datetime is more recent
                if row_datetime > existing_datetime:
                    # Update the map with the current row's title and datetime
                    position_title_map[row_position] = (row_title, row_datetime)
        else:
            position_title_map[row_position] = (row_title, row_datetime)

    # Filter rows based on position and title
    filtered_rows = []
    for row in rows:
        row_position = row[7]  # Assuming position is at index 7
        row_title = row[2]  # Assuming title is at index 2

        if row_position not in position_title_map:
            # Keep rows with unique positions
            filtered_rows.append(row)
        elif row_title == position_title_map[row_position][0]:
            # Keep rows with the same position and title
            filtered_rows.append(row)

    # Write the filtered rows back to the CSV file
    with open('/app/data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerows(filtered_rows)

train_regression_model()

print("Reading from Kafka...")
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafkaServer) \
    .option("subscribe", topic) \
    .load()

print("Writing to CSV...")
query = df.selectExpr("CAST(timestamp AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .foreach(write_to_csv) \
    .start() \
    .awaitTermination()
print("Done.")
