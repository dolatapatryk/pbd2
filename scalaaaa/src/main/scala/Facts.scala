import java.sql.Timestamp
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

import scala.util.control.Breaks._

object Facts {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)

    val conf: SparkConf = new SparkConf().
      //      setMaster("local").
      setAppName("facts")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    val getDateIdUdf = udf(getDateId)
    val getFakeDateUdf = udf(getFakeDate)
    val getHashUdf = udf(getHash)
    val vehicleTypes = Seq((1, "pedal_cycles"), (2, "two_wheeled_motor_vehicles"), (3, "cars_and_taxis"),
      (4, "buses_and_coaches"), (5, "lgvs"), (6, "all_hgvs"), (7, "all_motor_vehicles"))

    val weatherFile = spark.read.textFile(inputDirectory + "weather.txt")
    val weather = weatherFile.rdd.
      map(weather => {
        val splitted = weather.split(" ")
        val s = splitted(6).split("/")
        val hourSplitted = splitted(8).split(":")
        var r = s(2) + s(1) + s(0)
        if (hourSplitted.length < 2) {
          r = r + "0000"
        } else {
          r = r + hourSplitted(0) + hourSplitted(1);
        }
        (r.toLong, splitted(4), mapWeatherLine(weather))
      }).toDF("date", "authority", "weather")
    spark.sql("use traffic")
    val weatherTable = spark.sql("select id, weather from weathers")
    //    val weatherTable = getFikcyjnaTabela(weatherFile, spark)
    val joinedWeather = weather.join(weatherTable, weather("weather") === weatherTable("weather"))
      .orderBy("authority", "date").collect()
    val northEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataNorthEngland.csv")
    val scotlandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataScotland.csv")
    val southEnglandMainData = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "mainDataSouthEngland.csv")
    val mainData = northEnglandMainData.
      union(scotlandMainData).union(southEnglandMainData)
    //    val mainData = scotlandMainData
    val x = mainData.select("count_point_id", "local_authoirty_ons_code", "count_date", "hour").
      withColumn("date", getFakeDateUdf($"count_date", $"hour")).
      withColumnRenamed("local_authoirty_ons_code", "local_authority_ons_code").rdd.
      map(e => {
        (e.getInt(e.fieldIndex("count_point_id")), e.getLong(e.fieldIndex("date")),
          e.getString(e.fieldIndex("local_authority_ons_code")))
      }).toDF("count_point_id", "date", "local_authority_ons_code").
      orderBy("local_authority_ons_code", "date")

    var list: List[(Int, Int)] = List()
    var i = 0
    val dateIndex = joinedWeather(0).fieldIndex("date")
    val authorityIndex = joinedWeather(0).fieldIndex("authority")
    val indexId = joinedWeather(0).fieldIndex("id")
    val xy = x.distinct().collect()
    xy.foreach(row => {
      val rowDate = row.getLong(row.fieldIndex("date"))
      val countPointId = row.getInt(row.fieldIndex("count_point_id"))
      var weather = joinedWeather(i)
      val rowAuthority = row.getString(row.fieldIndex("local_authority_ons_code"))
      var weatherAuthority = weather.getString(authorityIndex)
      //skipujemy dopoki zaczna sie pogody dla naszego regionu
      breakable {
        while (!rowAuthority.equals(weather.getString(authorityIndex)) &&
          rowAuthority > weatherAuthority) {
          if (i == joinedWeather.length - 1) {
            break
          }
          i = i + 1
          weather = joinedWeather(i)
        }
      }
      //szukamy pierwszej wiekszej pogody
      while (joinedWeather.length > i && weather.getLong(dateIndex) < rowDate &&
        weather.getString(authorityIndex).equals(rowAuthority)) {
        i = i + 1
        weather = joinedWeather(i)
      }
      if (joinedWeather.length > i) {
        var previousWeather = joinedWeather(i)
        if (i > 0) {
          previousWeather = joinedWeather(i - 1)
        }
        var weatherId = -1
        val previousAuthority = previousWeather.getString(authorityIndex)
        weatherAuthority = weather.getString(authorityIndex)
        // moze sie zdarzyc ze weather nalezy juz do nastepnego regionu wiec wtedy bierzemy wartosc z previous
        if (previousAuthority.equals(rowAuthority) && weatherAuthority.equals(rowAuthority)) {
          weatherId = getClosestWeatherId(previousWeather.getLong(dateIndex),
            previousWeather.getInt(indexId), weather.getLong(dateIndex), weather.getInt(indexId),
            rowDate)
        } else if (previousAuthority.equals(rowAuthority)) {
          if (rowDate - previousWeather.getLong(dateIndex) <= 50000) {
            weatherId = previousWeather.getInt(indexId)
          }
        }
        val hash = (countPointId, rowDate, rowAuthority).hashCode()
        list = list :+ ((hash, weatherId))
      }
    })
    val listDS = list.toDS().withColumnRenamed("_1", "hash").
      withColumnRenamed("_2", "weatherId").distinct()
    val result = mainData.select("count_point_id", "direction_of_travel", "count_date", "hour",
      "pedal_cycles", "local_authoirty_ons_code", "two_wheeled_motor_vehicles",
      "cars_and_taxis", "buses_and_coaches", "lgvs", "all_hgvs", "all_motor_vehicles").
      withColumn("date", getDateIdUdf($"count_date")).
      withColumn("fake_date", getFakeDateUdf($"count_date", $"hour")).
      withColumn("hash", getHashUdf($"count_point_id", $"fake_date", $"local_authoirty_ons_code")).
      withColumnRenamed("local_authoirty_ons_code", "local_authority_ons_code")

    val mainJoinWithWeather = result.join(listDS, "hash").
      drop("hash", "count_point_id", "direction_of_travel", "count_date", "fake_date").
      withColumnRenamed("date", "dateId")

    // local_authority_ons_code, weatherId, dateId, hour, vehicle_type_id, count
    mainJoinWithWeather.foreach(row => {
      val local_authority_ons_code = row.getString(row.fieldIndex("local_authority_ons_code"))
      val weatherId = row.getInt(row.fieldIndex("weatherId"))
      val dateId = row.getInt(row.fieldIndex("dateId"))
      val hour = row.getInt(row.fieldIndex("hour"))
      vehicleTypes.foreach(e => {
        val vehicleTypeId = e._1
        var count = 0
        if (vehicleTypeId == 1 || vehicleTypeId == 2) {
          count = row.getDouble(row.fieldIndex(e._2)).toInt
        } else {
          count = row.getInt(row.fieldIndex(e._2))
        }

        spark.createDataset(Seq((local_authority_ons_code, weatherId, dateId, hour, vehicleTypeId, count))).
          withColumnRenamed("_1", "local_authority_ons_code").
          withColumnRenamed("_2", "weatherId").
          withColumnRenamed("_3", "dateId").
          withColumnRenamed("_4", "hour").
          withColumnRenamed("_5", "vehicle_type_id").
          withColumnRenamed("_6", "count").
          write.mode("append").insertInto("facts")
      })
    })
  }

  def getDateId: Date => Int = (date: Date) => {
    val calendar = Calendar.getInstance()
    calendar.setTime(date)
    (calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH) + 1, calendar.get(Calendar.DATE)).hashCode()
  }

  def getFakeDate: (Timestamp, Int) => Long = (date: Timestamp, hour: Int) => {
    var formattedDate = date.toString.substring(0, 10).
      replace("-", "")
    if (hour < 10) {
      formattedDate = formattedDate + "0"
    }
    (formattedDate + hour + "00").toLong
  }

  def getHash: (Int, Long, String) => Int = (countPointId: Int, date: Long, authority: String) => {
    (countPointId, date, authority).hashCode()
  }

  def mapWeatherLine(line: String): String = {
    val splitted = line.split(" ")
    var weather = ""
    for (i <- 15 until splitted.size) {
      weather = weather + splitted(i) + " "
    }
    weather.substring(0, weather.length - 1)
  }

  //  def getFikcyjnaTabela(weatherFile: Dataset[String], spark: SparkSession): DataFrame = {
  //    import spark.implicits._
  //    val weathers = weatherFile.rdd.map(line => mapWeatherLine(line)).distinct()
  //    weathers.map(weather => {
  //      (weather.hashCode, weather)
  //    }).toDF("id", "weather")
  //  }

  def getClosestWeatherId(lowerWeatherDate: Long, lowerWeatherId: Int, higherWeatherDate: Long, higherWeatherId: Int, referenceDate: Long): Int = {
    if (referenceDate - lowerWeatherDate > higherWeatherDate - referenceDate)
      if (higherWeatherDate - referenceDate > 50000) {
        -1
      } else {
        higherWeatherId
      }
    else {
      if (referenceDate - lowerWeatherDate > 50000) {
        -1
      } else {
        lowerWeatherId
      }
    }
  }
}
