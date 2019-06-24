package main.scala

import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.first
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions.udf

/**
 * TPC-H Query 19
 * Savvas Savvides <savvas@purdue.edu>
 *
 */
class Q19(lineitem: DataFrame, customer: DataFrame, order: DataFrame, part: DataFrame, partsupp: DataFrame, nation: DataFrame, region: DataFrame, supplier: DataFrame) extends TpchQuery(lineitem, customer, order, part, partsupp, nation, region, supplier) {

  override def execute(sc: SparkContext): DataFrame = {

    // this is used to implicitly convert an RDD to a DataFrame.
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    

    val sm = udf { (x: String) => x.matches("SM CASE|SM BOX|SM PACK|SM PKG") }
    val md = udf { (x: String) => x.matches("MED BAG|MED BOX|MED PKG|MED PACK") }
    val lg = udf { (x: String) => x.matches("LG CASE|LG BOX|LG PACK|LG PKG") }

    val decrease = udf { (x: Double, y: Double) => x * (1 - y) }
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


    // project part and lineitem first?
    p_part.join(l_lineitem, $"l_partkey" === $"p_partkey")
      .filter(($"l_shipmode" === "AIR" || $"l_shipmode" === "AIR REG") &&
        $"l_shipinstruct" === "DELIVER IN PERSON")
      .filter(
        (($"p_brand" === "Brand#12") &&
          sm($"p_container") &&
          $"l_quantity" >= 1 && $"l_quantity" <= 11 &&
          $"p_size" >= 1 && $"p_size" <= 5) ||
          (($"p_brand" === "Brand#23") &&
            md($"p_container") &&
            $"l_quantity" >= 10 && $"l_quantity" <= 20 &&
            $"p_size" >= 1 && $"p_size" <= 10) ||
            (($"p_brand" === "Brand#34") &&
              lg($"p_container") &&
              $"l_quantity" >= 20 && $"l_quantity" <= 30 &&
              $"p_size" >= 1 && $"p_size" <= 15))
      .select(decrease($"l_extendedprice", $"l_discount").as("volume"))
      .agg(sum("volume"))
  }

}
