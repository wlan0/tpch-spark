package main.scala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import org.apache.spark.sql._
import scala.collection.mutable.ListBuffer

/**
 * Parent class for TPC-H queries.
 *
 * Defines schemas for tables and reads pipe ("|") separated text files into these tables.
 *
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
abstract class TpchQuery(lineitem: DataFrame, customer: DataFrame, order: DataFrame, part: DataFrame, partsupp: DataFrame, nation: DataFrame, region: DataFrame, supplier: DataFrame) {

  // get the name of the class excluding dollar signs and package
  private def escapeClassName(className: String): String = {
    className.split("\\.").last.replaceAll("\\$", "")
  }

  def getName(): String = escapeClassName(this.getClass.getName)

  /**
   *  implemented in children classes and hold the actual query
   */
  def execute(sc: SparkContext): DataFrame
}

object TpchQuery {

  def outputDF(df: DataFrame, outputDir: String, className: String): Unit = {

    if (outputDir == null || outputDir == "")
      df.collect().foreach(println)
    else
      //df.write.mode("overwrite").json(outputDir + "/" + className + ".out") // json to avoid alias
      df.write.mode("overwrite").format("com.databricks.spark.csv").option("header", "true").save(outputDir + "/" + className)
  }

  def executeQueries(sc: SparkContext, queryNum: Int): ListBuffer[(String, Float)] = {

    // if set write results to hdfs, if null write to stdout
    // val OUTPUT_DIR: String = "/tpch"
    val OUTPUT_DIR: String = "s3a://xxxxxx/results/" + new File(".").getAbsolutePath() + "/dbgen/output"

    val results = new ListBuffer[(String, Float)]

    var fromNum = 1;
    var toNum = 22;
    if (queryNum != 0) {
      fromNum = queryNum;
      toNum = queryNum;
    }
    
    val spark = SparkSession.builder.getOrCreate()

    val lineitem = spark.read.orc("s3a://xxxxxx/lineitem/")
    val customer = spark.read.orc("s3a://xxxxxx/customer/")
    val order = spark.read.orc("s3a://xxxxxx/orders/")
    val part = spark.read.orc("s3a://xxxxxx/part/")
    val partsupp = spark.read.orc("s3a://xxxxxx/partsupp/")
    val nation = spark.read.orc("s3a://xxxxxx/nation/")
    val region = spark.read.orc("s3a://xxxxxx/region/")
    val supplier = spark.read.orc("s3a://xxxxxx/supplier/")

    val args = Array[AnyRef](lineitem, customer, order, part, partsupp, nation, region, supplier)
    
    for (queryNo <- fromNum to toNum) {
      val t0 = System.nanoTime()

      val query = Class.forName(f"main.scala.Q${queryNo}%02d").getDeclaredConstructor(classOf[DataFrame], classOf[DataFrame], classOf[DataFrame], classOf[DataFrame], classOf[DataFrame], classOf[DataFrame], classOf[DataFrame], classOf[DataFrame]).newInstance(args:_*).asInstanceOf[TpchQuery]

      outputDF(query.execute(sc), OUTPUT_DIR, query.getName())

      val t1 = System.nanoTime()

      val elapsed = (t1 - t0) / 1000000000.0f // second
      results += new Tuple2(query.getName(), elapsed)

    }

    return results
  }

  def main(args: Array[String]): Unit = {

    var queryNum = 0;
    if (args.length > 0)
      queryNum = args(0).toInt

    val conf = new SparkConf().setAppName("Spark TPC-H benchmark")
    val sc = new SparkContext(conf)

    // read files from local FS
    val INPUT_DIR = "s3a://xxxxxx/"

    // read from hdfs
    // val INPUT_DIR: String = "/dbgen"

    //val schemaProvider = new TpchSchemaProvider(sc, INPUT_DIR)

    val output = new ListBuffer[(String, Float)]
    output ++= executeQueries(sc, queryNum)

    val outFile = new File("s3a://xxxxx/results/TIMES.txt")
    val bw = new BufferedWriter(new FileWriter(outFile, true))

    output.foreach {
      case (key, value) => bw.write(f"${key}%s\t${value}%1.8f\n")
    }

    bw.close()
  }
}
