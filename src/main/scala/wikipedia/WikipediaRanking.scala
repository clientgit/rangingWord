package wikipedia

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.split(' ').contains(lang)
}

object WikipediaRanking extends WikipediaRankingInterface {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val ss: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("wikiRankApp")
    .getOrCreate();

  import ss.implicits._

  val wikiLines: List[String] = WikipediaData.lines;
  val wikiArticle: List[WikipediaArticle] = wikiLines.map(WikipediaData.parse)
  val wikiRdd: RDD[WikipediaArticle] = ss.sparkContext.parallelize(wikiArticle).cache()


  /** Returns the number of articles on which the language `lang` occurs
    * Using method `aggregate` on RDD[T].
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int = {
    rdd.aggregate(0)(
      (accumulator:Int, article:WikipediaArticle) => accumulator + (if (article.mentionsLanguage(lang)) 1 else 0),
      (accumulator1: Int, accumulator2: Int) => accumulator1 + accumulator2
    )
  }

  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    langs
      .map((lang: String) => (lang, occurrencesOfLang(lang, rdd)))
      .sortBy((record: (String, Int)) => record._2)
      .reverse
  }

  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    rdd
      .map((article: WikipediaArticle) => article.text.split(' ')
        .filter((world: String) => langs.contains(world))
        .map((world: String) => (world, article)))
      .flatMap((record: Array[(String, WikipediaArticle)]) => record)
      .groupByKey()
  }

  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    index
      .map((pair: (String, Iterable[WikipediaArticle])) => (pair._1, pair._2.size))
      .collect()
      .toList
      .sortBy((record: (String, Int)) => record._2)
      .reverse
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd
      .map((article: WikipediaArticle) => article.text.split(' ')
        .filter((world: String) => langs.contains(world))
        .map((world: String) => (world, 1)))
      .flatMap((record: Array[(String, Int)]) => record)
      .reduceByKey((val1: Int, val2: Int) => val1 + val2)
      .collect()
      .toList
      .sortBy((record: (String, Int)) => record._2)
      .reverse
  }

  def main(args: Array[String]): Unit = {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    val schema = StructType( Array(
      StructField("language", StringType, nullable = false),
      StructField("count", IntegerType, nullable = false))
    )

    val rows = langsRanked3.map((record: (String, Int)) => Row(record._1, record._2))
    val rdd = ss.sparkContext.parallelize(rows)
    val df3 = ss.createDataFrame(rdd, schema)
    df3.printSchema()
    df3.show()

    /* Output the speed of each ranking */
    println(timing)
    ss.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }

}
