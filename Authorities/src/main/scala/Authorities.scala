import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Authorities {

  def main(args: Array[String]): Unit = {
    val inputDirectory = args(0)

    val conf: SparkConf = new SparkConf().
//      setMaster("local").
      setAppName("authorities")
    val spark: SparkSession = SparkSession.builder().
      config(conf).
      enableHiveSupport().
      getOrCreate()

    val authNorthEngland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "authoritiesNorthEngland.csv")
    val authScotland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "authoritiesScotland.csv")
    val authSouthEngland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "authoritiesSouthEngland.csv")
    val regNorthEngland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "regionsNorthEngland.csv")
    val regScotland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "regionsScotland.csv")
    val regSouthEngland = spark.read.format("org.apache.spark.csv").
      option("header", "true").option("inferSchema", "true").
      csv(inputDirectory + "regionsSouthEngland.csv")

    val files_out_NorthEngland = authNorthEngland.alias("auth").
      join(regNorthEngland, authNorthEngland("region_ons_code").equalTo(regNorthEngland("region_ons_code")), "inner").
      select("local_authority_ons_code", "local_authority_name", "auth.region_ons_code", "region_name")
    val files_out_Scotland = authScotland.alias("auth").
      join(regScotland, authScotland("region_ons_code").equalTo(regScotland("region_ons_code")), "inner").
      select("local_authority_ons_code", "local_authority_name", "auth.region_ons_code", "region_name")
    val files_out_SouthEngland = authSouthEngland.alias("auth").
      join(regSouthEngland, authSouthEngland("region_ons_code").equalTo(regSouthEngland("region_ons_code")), "inner").
      select("local_authority_ons_code", "local_authority_name", "auth.region_ons_code", "region_name")

    val authorities = files_out_NorthEngland.
      union(files_out_Scotland).
      union(files_out_SouthEngland)

    spark.sql("use traffic")
    authorities.
      select("local_authority_ons_code", "local_authority_name", "region_ons_code", "region_name").
      write.mode("overwrite").saveAsTable("authorities")
  }
}
