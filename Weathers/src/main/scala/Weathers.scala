import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Weathers {
  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)

    val conf: SparkConf = new SparkConf().
//      setMaster("local").
      setAppName("weather")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    val weather = spark.read.textFile(inputDirectory + "weather.txt").rdd.
      map(line => mapWeatherLine(line)).
      distinct().
      map(weather => {
        (weather.hashCode, weather)
      }).toDS().
      withColumnRenamed("_1", "id").
      withColumnRenamed("_2", "weather")

    spark.sql("use traffic")
    weather.
      select($"id", $"weather").
      write.mode("overwrite").saveAsTable("weathers")
  }

  def mapWeatherLine(line: String): String = {
    val splitted = line.split(" ")
    var weather = ""
    for (i <- 15 until splitted.size) {
      weather = weather + splitted(i) + " "
    }

    weather.substring(0, weather.length - 1)
  }
}
