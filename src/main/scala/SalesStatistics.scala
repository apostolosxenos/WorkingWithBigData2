import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SalesStatistics {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("SalesStatistics")
      .getOrCreate()

    import ss.implicits._

    val inputFile = "input/sales.csv"

    // Read the contents of the csv file in a dataframe.
    // The csv file contains a header and columns have the correct type
    val df = ss.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load(inputFile)
      .withColumn("TotalPrice", $"Quantity" * $"UnitPrice").cache()

    // 4.1 Top 5 invoices with highest price
    val topFiveHighestPriceInvoices = df
      .groupBy($"InvoiceNo")
      .sum("TotalPrice")
      .orderBy(desc("sum(TotalPrice)"))
      .limit(5)
      .show()

    // 4.2 Top 5 most popular products
    val topFiveMostPopularProducts = df
      .groupBy($"StockCode")
      .sum("Quantity")
      .orderBy(desc("sum(Quantity)"))
      .limit(5)
      .show()

    // 4.3
    val topFiveMostPopularProductsIncludedInMostInvoices = df
      .select($"InvoiceNo", $"StockCode")
      .distinct()
      .groupBy($"StockCode")
      .agg(count($"StockCode"))
      .orderBy(desc("count(StockCode)"))
      .limit(5)
      .show()

    // 4.4
    val numberOfInvoices = df
      .select($"InvoiceNo")
      .distinct()
      .count()

    val meanInvoiceData = df
      .agg(sum($"Quantity").divide(numberOfInvoices).as("Mean Invoice Products Quantity"),
        sum($"TotalPrice").divide(numberOfInvoices).as("Mean Invoice Cost"))
      .show();


    // 4.5
    val topFiveCustomers = df
      .where($"CustomerId".isNotNull)
      .groupBy($"CustomerId")
      .sum("TotalPrice")
      .orderBy(desc("sum(TotalPrice)"))
      .limit(5)
      .show()

    ss.stop()
  }
}