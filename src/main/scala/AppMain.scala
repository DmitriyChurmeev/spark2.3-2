import org.apache.spark.sql.functions.{column, when}
import org.apache.spark.sql.SparkSession

object AppMain {

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Spark2.3-2")
      .master("local")
      .getOrCreate()

    val bikeSharingDF = sparkSession.read
      .option("inferSchema", "true")
      .option("header", "true")
      .option("sep", ",")
      .csv("src/main/resources/bike_sharing.csv")

    bikeSharingDF.select(
      column("HOLIDAY"),
      column("FUNCTIONING_DAY"))
      .withColumn("is_workday",
        when(column("HOLIDAY") === "Holiday"
          && column("FUNCTIONING_DAY") === "No", 0)
          .otherwise(1))
      .distinct()
      .show()
  }
}
