import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.catalyst.expressions.UnixTimestamp
import org.apache.spark.sql.functions.{col, count, desc, floor, from_unixtime, hour, lit, max, sum, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession, functions}

import java.sql.Timestamp
import java.text.SimpleDateFormat
import scala.util.{Failure, Success, Try}


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


    val simpleData = Seq(Row(123, 1667627426, "click", 101, "Sport", false),
      Row(123, 1667327426, "visit", 111, "TV", true),
      Row(111, 1667624426, "scroll", 131, "Love", false),
      Row(222, 1637627426, "move", 201, "News", true),
      Row(333, 1667627426, "click", 201, "Sport", false),
      Row(123, 1667327426, "visit", 111, "TV", true),
      Row(444, 1667624426, "scroll", 131, "Love", false),
      Row(777, 1637627426, "move", 201, "News", true),
      Row(111, 1667627426, "click", 201, "Sport", false),
      Row(777, 1667327426, "visit", 111, "TV", true),
      Row(888, 1667624426, "scroll", 131, "Love", false),
      Row(123, 1637627426, "click", 201, "News", true),
      Row(545, 1667627426, "click", 121, "Sport", false),
      Row(222, 1667327426, "visit", 111, "TV", true),
      Row(888, 1667624426, "scroll", 131, "Love", false),
      Row(555, 1637627426, "move", 201, "News", true),
      Row(123, 1667627426, "click", 111, "Sport", false),
      Row(999, 1667327426, "visit", 111, "TV", true),
      Row(666, 1667624426, "scroll", 131, "Love", false),
      Row(132, 1637627426, "move", 201, "News", true),
      Row(123, 1662327426, "click", 401, "Action", false)
    )

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData), schema)
    df.show(false)

    val most_active_users = df
      .groupBy("id")
      .agg(count("id").as("countId"))
      .select("id", "countId").orderBy(desc("countId"))
    //      .show(5)

    val percent_sign_users = df
      .groupBy("sign")
      .agg(
        count("*").as("allCountUsers"))
      .withColumn("percent", col("allCountUsers") * 100 / sum("allCountUsers").over())
    //      .show(false)

    df.filter(col("type") === "click")
      .groupBy("page_id")
      .agg(count("page_id").as("countPage"))
      .select("page_id", "countPage").orderBy(desc("countPage"))
    //      .show(5)

   val timeRangeDf = df
//     .select("time_visit")
     .withColumn("time_range", hour(from_unixtime(col("time_visit"))) / 4)

//   timeRangeDf.groupBy("time_range").agg(count("time_range").alias("count"))
//     .groupBy().agg(max("count").alias("max_actions"))
//     .show()

    timeRangeDf.groupBy("time_range").agg(count("time_range").alias("count"))
//      .groupBy().agg(max("count").alias("max_actions"))
      .select(
        "time_range",
        "count"
      ).orderBy( desc("count"))
//      .show(1)
      .show(false)

  }
}
/*
d.       Решите следующие задачи:
+   Вывести топ-5 самых активных посетителей сайта
·   Посчитать процент посетителей, у которых есть ЛК
·   Вывести топ-5 страниц сайта по показателю общего кол-ва кликов на данной странице
·   Добавьте столбец к фрейму данных со значением временного диапазона в рамках суток с размером окна – 4 часа(0-4, 4-8, 8-12 и т.д.)
·   Выведите временной промежуток на основе предыдущего задания, в течение которого было больше всего активностей на сайте.
·   Создайте второй фрейм данных, который будет содержать информацию о ЛК посетителя сайта со следующим списком атрибутов
1.       Id – уникальный идентификатор личного кабинета
2.       User_id – уникальный идентификатор посетителя
3.       ФИО посетителя
4.    Дату рождения посетителя
5.       Дата создания ЛК
·   Вывести фамилии посетителей, которые читали хотя бы одну новость про спорт.
 */