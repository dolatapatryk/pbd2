import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object VehicleTypes {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().
      //            setMaster("local").
      setAppName("vehicleTypes")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()
    import spark.implicits._
    val types = Seq((1, "pedal_cycles"), (2, "two_wheeled_motor_vehicles"), (3, "cars_and_taxis"),
      (4, "buses_and_coaches"), (5, "lgvs"), (6, "all_hgvs"), (7, "all_motor_vehicles")).
      toDF("type_id", "type_name")

    spark.sql("use traffic")
    types.
      select($"type_id", $"type_name").
      write.mode("overwrite").saveAsTable("vehicle_types")
  }
}
