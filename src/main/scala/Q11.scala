package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 11
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q11(lineitem: DataFrame, customer: DataFrame, order: DataFrame, part: DataFrame, partsupp: DataFrame, nation: DataFrame, region: DataFrame, supplier: DataFrame) extends TpchQuery(lineitem, customer, order, part, partsupp, nation, region, supplier) {

  override def execute(sc: SparkContext): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    

    val mul = udf { (x: Double, y: Int) => x * y }
    val mul01 = udf { (x: Double) => x * 0.0001 }
    val n_nation_cols = nation.columns.map(c=> nation(c).as(s"n_$c"))
    val n_nation = nation.select(n_nation_cols: _*)

    val r_region_cols = region.columns.map(c=> region(c).as(s"r_$c"))
    val r_region = region.select(r_region_cols: _*)

    val s_supplier_cols = supplier.columns.map(c=> supplier(c).as(s"s_$c"))
    val s_supplier = supplier.select(s_supplier_cols: _*)

    val p_part_cols = part.columns.map(c=> part(c).as(s"p_$c"))
    val p_part = part.select(p_part_cols: _*)

    val ps_partsupp_cols = partsupp.columns.map(c=> partsupp(c).as(s"ps_$c"))
    val ps_partsupp = partsupp.select(ps_partsupp_cols: _*)
  
    val l_lineitem_cols = lineitem.columns.map(c=> lineitem(c).as(s"l_$c"))
    val l_lineitem = lineitem.select(l_lineitem_cols: _*)

    val c_customer_cols = customer.columns.map(c=> customer(c).as(s"c_$c"))
    val c_customer = customer.select(c_customer_cols: _*)

    val o_order_cols = order.columns.map(c=> order(c).as(s"o_$c"))
    val o_order = order.select(o_order_cols: _*)


    val tmp = n_nation.filter($"n_name" === "GERMANY")
      .join(s_supplier, $"n_nationkey" === s_supplier("s_nationkey"))
      .select($"s_suppkey")
      .join(ps_partsupp, $"s_suppkey" === ps_partsupp("ps_suppkey"))
      .select($"ps_partkey", mul($"ps_supplycost", $"ps_availqty").as("value"))
    // .cache()

    val sumRes = tmp.agg(sum("value").as("total_value"))

    tmp.groupBy($"ps_partkey").agg(sum("value").as("part_value"))
      .join(sumRes, $"part_value" > mul01($"total_value"))
      .sort($"part_value".desc)
  }

}
