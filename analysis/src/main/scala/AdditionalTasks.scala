import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.types._

object AdditionalTasks {
  def main(args: Array[String]) {
//===================siepam-11=========================
//    val spark: SparkSession = SparkSession
//      .builder.master("local")
//      .appName("Scala Spark Example")
//      .getOrCreate()
//
//    val flightData2015 = spark
//      .read
//      .option("inferSchema", true)
//      .option("header", true)
//      .csv("src/main/resources/2015-summary.csv")
//
//    flightData2015.createOrReplaceTempView("flight_data_2015")
//
//    val sqlWay = spark.sql("""
//           SELECT DEST_COUNTRY_NAME, count(1)
//           FROM flight_data_2015
//           GROUP BY DEST_COUNTRY_NAME
//           """)
//
//    val dataFrameWay = flightData2015
//      .groupBy("DEST_COUNTRY_NAME")
//      .count()
//    sqlWay.explain
//    dataFrameWay.explain
//======================================================
//====================siepam-14=========================
    val spark: SparkSession = SparkSession
      .builder.master("local")
      .appName("Scala Spark Example")
      .getOrCreate()

    import spark.implicits._

    val jsonSchema = new StructType()
      .add("venue",
        new StructType()
          .add("venue_name", StringType)
          .add("lon", DoubleType)
          .add("lat", DoubleType)
          .add("venue_id", LongType))
      .add("visibility", StringType)
      .add("response", StringType)
      .add("guests", LongType)
      .add("member",
        new StructType()
          .add("member_id", LongType)
          .add("photo", StringType)
          .add("member_name", StringType))
      .add("rsvp_id", LongType)
      .add("mtime", LongType)
      .add("event",
        new StructType()
          .add("event_name", StringType)
          .add("event_id", StringType)
          .add("time", LongType)
          .add("event_url", StringType))
      .add("group",
        new StructType()
          .add("group_city", StringType)
          .add("group_country", StringType)
          .add("group_id", LongType)
          .add("group_lat", DoubleType)
          .add("group_long", DoubleType)
          .add("group_name", StringType)
          .add("group_state", StringType)
          .add("group_topics", DataTypes.createArrayType(
            new StructType()
              .add("topicName", StringType)
              .add("urlkey", StringType)))
          .add("group_urlname", StringType))

    val streamingInputDF = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "kafkaserver:9092")
      .option("subscribe", "data_test")
      .load()

    val personStringDF = streamingInputDF.selectExpr("CAST(value AS STRING)")
    personStringDF.printSchema()

    val streamingSelectDF = personStringDF
      .select(from_json($"value", jsonSchema)
        .as("data")).select("data.*")

    streamingSelectDF.printSchema()

    streamingSelectDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
//====================================================
  }
}
