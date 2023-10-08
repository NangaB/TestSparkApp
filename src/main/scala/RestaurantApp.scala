import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object RestaurantApp {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("restaurant-application")
      .master("local")
      .getOrCreate()

    import sparkSession.implicits._

    val pizzaRawDF: Dataset[Row] = sparkSession.read
      .option("header", "true")
      .csv("pizza_data.csv")

//    pizzaRawDF.show()
//    pizzaRawDF.printSchema()

    val pizzaCleanDF : Dataset[Row] = pizzaRawDF.withColumn("Price", regexp_replace(col("Price"), pattern = "[$,]", "" ).cast(DataTypes.DoubleType))

    val companiesWithAvgPricesDF: Dataset[Row] = pizzaCleanDF.groupBy("Company").avg("Price")

    companiesWithAvgPricesDF.show()


  }

}
