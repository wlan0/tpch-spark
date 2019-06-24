package main.scala

import org.apache.spark.SparkContext

// TPC-H table schemas
case class Customer(
  custkey: Long,
  name: String,
  address: String,
  nationkey: Long,
  phone: String,
  acctbal: Double,
  mktsegment: String,
  comment: String)

case class Lineitem(
  orderkey: Long,
  partkey: Long,
  suppkey: Long,
  linenumber: Long,
  quantity: Double,
  extendedprice: Double,
  discount: Double,
  tax: Double,
  returnflag: String,
  linestatus: String,
  shipdate: String,
  commitdate: String,
  receiptdate: String,
  shipinstruct: String,
  shipmode: String,
  comment: String)

case class Nation(
  nationkey: Long,
  name: String,
  regionkey: Long,
  comment: String)

case class Order(
  orderkey: Long,
  custkey: Long,
  ordertatus: String,
  totalprice: Double,
  orderdate: String,
  orderpriority: String,
  clerk: String,
  shippriority: Long,
  comment: String)

case class Part(
  partkey: Long,
  name: String,
  mfgr: String,
  brand: String,
  xtype: String,
  size: Long,
  container: String,
  retailprice: Double,
  comment: String)

case class Partsupp(
  partkey: Long,
  suppkey: Long,
  availqty: Long,
  supplycost: Double,
  comment: String)

case class Region(
  regionkey: Long,
  name: String,
  comment: String)

case class Supplier(
  suppkey: Long,
  name: String,
  address: String,
  nationkey: Long,
  phone: String,
  acctbal: Double,
  comment: String)

class TpchSchemaProvider(sc: SparkContext, inputDir: String) {

  // this is used to implicitly convert an RDD to a DataFrame.
  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  import sqlContext.implicits._

  val dfMap = Map(
    "customer" -> sc.textFile(inputDir + "/customer.tbl*").map(_.split('|')).map(p =>
      Customer(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim, p(7).trim)).toDF(),

    "lineitem" -> sc.textFile(inputDir + "/lineitem.tbl*").map(_.split('|')).map(p =>
      Lineitem(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toLong, p(4).trim.toDouble, p(5).trim.toDouble, p(6).trim.toDouble, p(7).trim.toDouble, p(8).trim, p(9).trim, p(10).trim, p(11).trim, p(12).trim, p(13).trim, p(14).trim, p(15).trim)).toDF(),

    "nation" -> sc.textFile(inputDir + "/nation.tbl*").map(_.split('|')).map(p =>
      Nation(p(0).trim.toLong, p(1).trim, p(2).trim.toLong, p(3).trim)).toDF(),

    "region" -> sc.textFile(inputDir + "/region.tbl*").map(_.split('|')).map(p =>
      Region(p(0).trim.toLong, p(1).trim, p(2).trim)).toDF(),

    "order" -> sc.textFile(inputDir + "/order.tbl*").map(_.split('|')).map(p =>
      Order(p(0).trim.toLong, p(1).trim.toLong, p(2).trim, p(3).trim.toDouble, p(4).trim, p(5).trim, p(6).trim, p(7).trim.toLong, p(8).trim)).toDF(),

    "part" -> sc.textFile(inputDir + "/part.tbl*").map(_.split('|')).map(p =>
      Part(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim, p(4).trim, p(5).trim.toLong, p(6).trim, p(7).trim.toDouble, p(8).trim)).toDF(),

    "partsupp" -> sc.textFile(inputDir + "/partsupp.tbl*").map(_.split('|')).map(p =>
      Partsupp(p(0).trim.toLong, p(1).trim.toLong, p(2).trim.toLong, p(3).trim.toDouble, p(4).trim)).toDF(),

    "supplier" -> sc.textFile(inputDir + "/supplier.tbl*").map(_.split('|')).map(p =>
      Supplier(p(0).trim.toLong, p(1).trim, p(2).trim, p(3).trim.toLong, p(4).trim, p(5).trim.toDouble, p(6).trim)).toDF())

  // for implicits
  val customer = dfMap.get("customer").get
  val lineitem = dfMap.get("lineitem").get
  val nation = dfMap.get("nation").get
  val region = dfMap.get("region").get
  val order = dfMap.get("order").get
  val part = dfMap.get("part").get
  val partsupp = dfMap.get("partsupp").get
  val supplier = dfMap.get("supplier").get

  dfMap.foreach {
    case (key, value) => value.createOrReplaceTempView(key)
  }
}
