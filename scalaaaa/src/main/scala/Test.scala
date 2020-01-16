import java.lang.{ArrayIndexOutOfBoundsException, Exception}
import java.util.{Calendar, Date}

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

object Test {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      setMaster("local").
      setAppName("test")
    val sc: SparkContext = new SparkContext(conf)
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val weatherFile = spark.read.textFile("/home/patryk/pbd2/uk-trafic/weather.txt")
    val weather = weatherFile.rdd.
      map(weather => {
        val splitted = weather.split(" ")
        val s = splitted(6).split("/")
        val hourSplitted = splitted(8).split(":")
        var r = s(2) + s(1) + s(0)
        if(hourSplitted.length < 2) {
          r = r + "0000"
        } else {
          r = r + hourSplitted(0) + hourSplitted(1);
        }
        (r.toLong, splitted(4), mapWeatherLine(weather))
      }).toDF("date", "authority", "weather")
    //    val weatherTable = spark.sql("select id, weather from weather")
    val weatherTable = getFikcyjnaTabela(weatherFile, spark)
    val joinedWeather = weather.join(weatherTable, weather("weather") === weatherTable("weather"))
      .orderBy("authority", "date").collect()

    val northEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv("/home/patryk/pbd2/uk-trafic/mainDataNorthEngland.csv")
    val scotlandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv("/home/patryk/pbd2/uk-trafic/mainDataScotland.csv")
    val southEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv("/home/patryk/pbd2/uk-trafic/mainDataSouthEngland.csv")
    val mainData = northEnglandMainData.
      union(scotlandMainData).union(southEnglandMainData)
    val x = mainData.select("count_point_id", "year", "count_date", "hour",
      "pedal_cycles", "local_authority_ons_code", "pedal_cycles", "two_wheeled_motor_vehicles",
      "cars_and_taxis", "buses_and_coaches", "lgvs", "hgvs_2_rigid_axle", "hgvs_3_rigid_axle",
      "hgvs_4_or_more_rigid_axle", "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle",
      "hgvs_6_articulated_axle", "all_hgvs", "all_motor_vehicles").rdd.
      map(e => {
        var date = e.getTimestamp(e.fieldIndex("count_date")).toString.substring(0, 10).
          replace("-", "")
        val hour = e.getInt(e.fieldIndex("hour"))
        if (hour < 10) {
          date = date + "0"
        }
        (e.getInt(e.fieldIndex("count_point_id")), (date + hour + "00").toLong,
          e.getString(e.fieldIndex("local_authority_ons_code")))
      }).toDF("count_point_id", "date", "local_authority_ons_code").
      orderBy("local_authority_ons_code", "date")
    var i = 0
    var k = 0
    val dateIndex = joinedWeather(0).fieldIndex("date")
    val authorityIndex = joinedWeather(0).fieldIndex("authority")
    val indexId = joinedWeather(0).fieldIndex("id")
    x.collect().foreach(row => {
      val rowDate = row.getLong(row.fieldIndex("date"))
      var weather = joinedWeather(i)
      val rowAuthority = row.getString(row.fieldIndex("local_authority_ons_code"))
      while (joinedWeather.length > i && weather.getLong(dateIndex) < rowDate &&
        weather.getString(authorityIndex).equals(rowAuthority)) {
        weather = joinedWeather(i)
        i = i + 1
      }
      if (joinedWeather.length > i) {
        var previousWeather = joinedWeather(i - 1)
        val closestWeatherId = getClosestWeatherId(previousWeather.getLong(dateIndex),
          previousWeather.getInt(indexId), weather.getLong(dateIndex), weather.getInt(indexId),
          rowDate)
        println(rowDate + " " + previousWeather + " " + weather + " " + closestWeatherId)
        k = k + 1
        if(k > 20) {
          return
        }
      }
    })

    //        mainData.select("count_point_id", "year", "count_date", "hour",
    //          "pedal_cycles", "local_authority_ons_code", "pedal_cycles", "two_wheeled_motor_vehicles",
    //          "cars_and_taxis", "buses_and_coaches", "lgvs", "hgvs_2_rigid_axle", "hgvs_3_rigid_axle",
    //          "hgvs_4_or_more_rigid_axle", "hgvs_3_or_4_articulated_axle", "hgvs_5_articulated_axle",
    //          "hgvs_6_articulated_axle", "all_hgvs", "all_motor_vehicles").
    //      show(1)
  }

  def mapWeatherLine(line: String): String = {
    val splitted = line.split(" ")
    var weather = ""
    for (i <- 15 until splitted.size) {
      weather = weather + splitted(i) + " "
    }
    weather.substring(0, weather.length - 1)
  }

  def getFikcyjnaTabela(weatherFile: Dataset[String], spark: SparkSession): DataFrame = {
    import spark.implicits._
    val weathers = weatherFile.rdd.map(line => mapWeatherLine(line)).distinct()
    weathers.map(weather => {
      (weather.hashCode, weather)
    }).toDF("id", "weather")
  }

  def getClosestWeatherId(lowerWeatherDate: Long, lowerWeatherId: Int, higherWeatherDate: Long, higherWeatherId: Int, referenceDate: Long): Int = {
    if (referenceDate - lowerWeatherDate > 2 || higherWeatherDate - referenceDate > 2)
      return -1;
    if (referenceDate - lowerWeatherDate > higherWeatherDate - referenceDate)
      lowerWeatherId;
    else {
      higherWeatherId;
    }
  }
}
