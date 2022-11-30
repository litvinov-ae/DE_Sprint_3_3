import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}


object RDDParallelize {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("SparkApp")
      .getOrCreate()


    val schema = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("time_visit", IntegerType, true),
      StructField("type", StringType, true),
      StructField("page_id", IntegerType, true),
      StructField("tag", StringType, true),
      StructField("sign", BooleanType, true)))


    val simpleData = Seq(Row(1, 1667627426, "click", 101, "Sport", false),
      Row(1, 1667327426, "visit", 111, "TV", true),
      Row(2, 1667624426, "scroll", 131, "Love", false),
      Row(3, 1637627426, "move", 201, "News", true),
      Row(4, 1667627426, "click", 201, "Sport", false),
      Row(1, 1667327426, "visit", 111, "TV", true),
      Row(5, 1667624426, "scroll", 131, "Love", false),
      Row(7, 1637627426, "move", 201, "News", true),
      Row(2, 1667627426, "click", 201, "Sport", false),
      Row(7, 1667327426, "visit", 111, "TV", true),
      Row(8, 1667624426, "scroll", 131, "Love", false),
      Row(1, 1637627426, "click", 201, "News", true),
      Row(6, 1667627426, "click", 121, "Sport", false),
      Row(3, 1667327426, "visit", 111, "TV", true),
      Row(8, 1667624426, "scroll", 131, "Love", false),
      Row(15, 1637627426, "move", 201, "News", true),
      Row(1, 1667627426, "click", 111, "Sport", false),
      Row(11, 1667327426, "visit", 111, "TV", true),
      Row(12, 1667624426, "scroll", 131, "Love", false),
      Row(14, 1637627426, "move", 201, "News", true),
      Row(1, 1662327426, "click", 401, "Action", false)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), schema)
    df.show(false)

    val most_active_users = df
      .groupBy("id")
      .agg(count("id").as("countId"))
      .select("id", "countId").orderBy(desc("countId"))
      .show(5)

    val percent_sign_users = df
      .groupBy("sign")
      .agg(
        count("*").as("allCountUsers"))
      .withColumn("percent", col("allCountUsers") * 100 / sum("allCountUsers").over())
      .show(false)

    df.filter(col("type") === "click")
      .groupBy("page_id")
      .agg(count("page_id").as("countPage"))
      .select("page_id", "countPage").orderBy(desc("countPage"))
      .show(5)

    val timeRangeDf = df
      .withColumn("time_range", hour(from_unixtime(col("time_visit"))) / 4)


    timeRangeDf.groupBy("time_range").agg(count("time_range").alias("count"))
      .select(
        "time_range",
        "count"
      ).orderBy(desc("count"))
      .show(1)

    val usersData = Seq(Row(1, 17846781, "Smith", 1667627426, 1067627426),
      Row(2, 92646112, "James", 1667327426, 1067327426),
      Row(3, 13413413, "Bond", 1667624426, 1067624426),
      Row(4, 13413413, "Antony", 1637627426, 1037627426),
      Row(5, 42524524, "Master", 1667627426, 1067627426),
      Row(6, 45245766, "Margarita", 1667327426, 1067327426),
      Row(7, 63742444, "Ivanov", 1667624426, 1067624426),
      Row(8, 78094754, "Felimonov", 1637627426, 1037627426),
      Row(9, 98240952, "Evdokimov", 1667627426, 1067627426),
      Row(10, 53689866, "Robert", 1667327426, 1067327426),
      Row(11, 82385235, "Maria", 1667624426, 1067624426),
      Row(12, 89009847, "Jen", 1637627426, 1037627426),
      Row(13, 28759673, "Anne", 1667627426, 1067627426),
      Row(14, 43956455, "Rose", 1667327426, 1067327426),
      Row(15, 23423434, "Jones", 1667624426, 1067624426),
      Row(16, 34234234, "Brown", 1637627426, 1037627426),
      Row(17, 11111212, "Green", 1667627426, 1067627426),
      Row(18, 32423434, "Kleopatra", 1667327426, 1067327426),
      Row(19, 54545454, "Kate", 1667624426, 1067624426),
      Row(20, 53452232, "Agent", 1637627426, 1037627426),
      Row(21, 23213232, "Morfius", 1662327426, 1062327426)
    )

    val schemaUsers = StructType(Array(
      StructField("id", IntegerType, true),
      StructField("user_id", IntegerType, true),
      StructField("user_name", StringType, true),
      StructField("birst_day", IntegerType, true),
      StructField("lc_create_date", IntegerType, true)))


    val usersDf = spark.createDataFrame(spark.sparkContext.parallelize(usersData), schemaUsers)
    usersDf.show(false)

    val sportUsers = df
      .filter(col("tag") === "Sport")
      .join(usersDf, Seq("id"), "left")
      .select("user_name")
      .distinct()
      .show(false)


  }
}