import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]): Unit = {

    val ss = SparkSession.builder
      .master("local")
      .appName("WordCount")
      .getOrCreate()

    val inputFile = "input/Shakespeare.txt"
    val inputFile2 = "input/SherlockHolmes.txt"

    // Creating data RDD from file
    val dataRDD = ss.sparkContext.textFile(inputFile)

    /*
     Main RDD Transformation =>
     Removing punctuation,
     Splitting by white space/s,
     Trimming,
     Filtering non-empty lines,
     Lower casing
    */
    val formattedData = dataRDD
      .map(line => line.replaceAll("[^A-Za-z0-9]"," "))
      .flatMap(line => line.split("\\s+"))
      .map(line => line.trim)
      .filter(line => line.nonEmpty)
      .map(line => line.toLowerCase)

    // 1.1 Total number of words, Action: count
    val numberOfWords = formattedData
      .count()
    println(s"Number of words: " + numberOfWords)

    // 1.2 Words' mean length, Action: sum
    val wordsLengthSum = formattedData
      .map(word => word.size)
      .sum()

    val meanWordLength = wordsLengthSum / numberOfWords
    println(s"Words' mean length: " + meanWordLength)

    // 1.3 RDD Transformation =>
    // Five most frequent words whose length is more than average, sorted by value in descending order
    val fiveMostFrequentWords = formattedData
      .filter(word => word.size > meanWordLength)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)
      .take(5)
    println(s"Five most frequent words whose length is more than average, sorted by value in descending order: ")
    fiveMostFrequentWords.foreach(println)

    ss.stop();

  }
}