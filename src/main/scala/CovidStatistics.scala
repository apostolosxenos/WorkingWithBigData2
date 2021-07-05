import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object CovidStatistics {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder()
      .master("local")
      .appName("CovidStatistics")
      .getOrCreate()

    import ss.implicits._

    val inputFile = "input/covid.csv"

    // Read the contents of the csv file in a dataframe.
    // The csv file contains a header and columns have the correct type
    val df = ss.read
      .format("csv")
      .options(Map("inferSchema" -> "true", "header" -> "true"))
      .load(inputFile).cache()

    // 3.1
    val casesInGreeceForDecember2020 = df
      .select($"countriesAndTerritories", $"year", $"month", $"day", $"cases")
      .where($"year" === 2020 && $"month" === 12 && $"countriesAndTerritories" === "Greece")

    val count31 = casesInGreeceForDecember2020.count().toInt // Long to Int casting
    casesInGreeceForDecember2020.show(count31)

    // 3.2
    val totalCasesAndDeathsForEachContinent = df
      .groupBy("continentExp")
      .sum("cases", "deaths")

    totalCasesAndDeathsForEachContinent.show()

    // 3.3
    val dailyMeanCasesAndDeathsInEachEuropeanCountry = df
      .where($"continentExp" === "Europe")
      .groupBy("countriesAndTerritories")
      .avg("cases", "deaths")
      .orderBy("countriesAndTerritories")

    val count33 = dailyMeanCasesAndDeathsInEachEuropeanCountry.count().toInt // Long to Int casting
    dailyMeanCasesAndDeathsInEachEuropeanCountry.show(count33)

    // 3.4
    val worstDatesAccordingToCasesInEurope = df
      .where($"continentExp" === "Europe")
      .groupBy($"dateRep")
      .sum("cases")
      .orderBy(desc("sum(cases)"))
      .limit(10)

    worstDatesAccordingToCasesInEurope.show()


    ss.stop()
  }
}