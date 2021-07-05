import org.apache.spark.sql.SparkSession

object ChessStatistics {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .master("local")
      .appName("ChessStatistics")
      .getOrCreate()

    val inputFile = "input/games.csv"

    // Creating data RDD from file
    val gamesRDD = ss.sparkContext.textFile(inputFile)

    // Filter out the header row
    val header = gamesRDD.first()
    val splitGamesData = gamesRDD
      .filter(row => row != header)
      .map(row => row.split(",")).cache()

    // 2.1
    val playersPairsRDD = splitGamesData
      .map(row => {
        val whitePlayerId = row(8)
        val blackPlayerId = row(10)

        // Comparing two strings lexicographically
        // Then build a string where the left most id has smaller value than the right most id
        // Using this way we can easily identify same pairs
        // For example b__a = a__b
        if (whitePlayerId.compareTo(blackPlayerId) < 0) {
          whitePlayerId + "__" + blackPlayerId
        }
        else {
          blackPlayerId + "__" + whitePlayerId
        }
      })

    // 2.2
    val pairsPlayedMoreThanFiveTimesTogether = playersPairsRDD
      .map(row => (row, 1))
      .reduceByKey(_ + _)
      .filter(row => row._2 > 5)
      .sortBy(_._2, false)

    pairsPlayedMoreThanFiveTimesTogether.foreach(println)

    // 2.3
    val allMoves = splitGamesData
      .flatMap(row => row(12).split("\\s+"))
      .map(move => (move, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)

    allMoves.foreach(println)

    ss.stop()
  }
}