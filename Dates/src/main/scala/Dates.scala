import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object Dates {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      //      setMaster("local").
      setAppName("dates")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._

    val startYear = args(0)
    val endYear = args(1)

    val start = Calendar.getInstance()
    start.set(Integer.valueOf(startYear), 0, 1);
    val end = Calendar.getInstance()
    end.set(Integer.valueOf(endYear), 11, 31);

    val dates = new ListBuffer[Date]()
    while (!start.after(end)) {
      dates += start.getTime
      start.add(Calendar.DATE, 1)
    }

    val cal = Calendar.getInstance()
    val list = dates.toList.
      map(date => {
        cal.setTime(date)
        val year = cal.get(Calendar.YEAR)
        val month = cal.get(Calendar.MONTH) + 1
        val day = cal.get(Calendar.DATE)
        ((year, month, day).hashCode(), year, month, day)
      }).toDS().
      withColumnRenamed("_1", "id").
      withColumnRenamed("_2", "year").
      withColumnRenamed("_3", "month").
      withColumnRenamed("_4", "day")

    spark.sql("use traffic")
    list.
      select($"id", $"year", $"month", $"day").
      write.mode("overwrite").saveAsTable("dates")
  }
}
