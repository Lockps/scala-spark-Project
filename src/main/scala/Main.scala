import org.apache.spark.sql.functions.{col, concat, current_timestamp, expr, lit}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Test")
      .master("local[*]")
      .config("spark.driver.blindAddress","127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header" , value = true)
      .option("inferSchema", value = true)
      .csv("data/AAPL.csv")

    df.show()
    df.printSchema()

    val column = df("Open")
    val newColumn = (column + 2.0).as("OpenIncreaseBy2")
    val columnString = column.cast(StringType).as("OpenAsString")

    val litColumn = lit(2.0)
    val newColumnStr = concat(columnString, lit("Hello World"))

    df.select(column,newColumn , columnString, newColumnStr , litColumn)
      .show(truncate = false)

    val timeStampfromexpr = expr("cast(current_timestamp() as string) as timestampfromexpr")
    val timeStampfromfunc = current_timestamp().cast(StringType).as("timestampfromfunction")

    df.select(timeStampfromexpr,timeStampfromfunc).show()

    df.selectExpr("cast(Date as string)", "Open + 1.0" , "current_timestamp()").show()

    df.createTempView("df")
    spark.sql("select * from df").show()


    df.withColumnRenamed("Open" , "open")
      .withColumnRenamed("Close" , "close")

    val renameColumns = List(
      col("Date").as("date"),
      col("Open").as("open"),
      col("High").as("high"),
      col("Low").as("low"),
      col("Close").as("close"),
      col("Adj Close").as("adjclose"),
      col("Volume").as("volume")
    )

    df.select(renameColumns: _*).show()

    df.columns.map(c => col(c).as(c.toLowerCase()))

    val stockData = df.select(renameColumns: _*)
      .withColumn("diff",col("close") - col("open"))
      .filter(col("close") > col("open") * 1.1)

    stockData.show()
  }
}