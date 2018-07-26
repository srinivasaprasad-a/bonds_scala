import scala.math.pow

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.ArrayType

import org.apache.spark.sql.functions.udf

object bonds {
  def main(args: Array[String]): Unit = {
    //Create SparkSession
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Bonds")
      .getOrCreate()

    //Set schema to read the input json file
    //Setting all StringType as the input for each variable is also String
    val bonds_json_schema = StructType(Array(StructField("id", StringType, true),
      StructField("value", StringType, true),
      StructField("coupon", StringType, true),
      StructField("yield", StringType, true),
      StructField("type", StringType, true),
      StructField("duration", StringType, true)))

    //Create DataFrame by loading input json file
    val bonds_df = spark.read.schema(bonds_json_schema).json("bonds.json")

    //Change the datatype of each variable/column and set to another DataFrame
    val bonds_cast_df = bonds_df
      .filter("id!='null'")
      .select(bonds_df("id"),
        bonds_df("value").cast(DecimalType(10, 2)),
        bonds_df("coupon").cast(DecimalType(10, 5)),
        bonds_df("yield").cast(DecimalType(10, 5)),
        bonds_df("type"),
        bonds_df("duration").cast(IntegerType))

    //bonds_cast_df.printSchema()
    bonds_cast_df.show()

    val periodic_value_schema = StructType(Array(StructField("period", IntegerType, true),
      StructField("cp", DecimalType(12, 5), true),
      StructField("pv", DecimalType(12, 5), true),
      StructField("aggpv", DecimalType(12, 5), true),
      StructField("quote", DecimalType(12, 5), true)))


    val map_calc_periodic_value = (subRows: Row) => {
      calc_periodic_value(subRows.getAs[DecimalType](1),subRows.getAs[DecimalType](2), subRows.getAs[DecimalType](3),
        subRows.getAs[StringType](4), subRows.getAs[IntegerType](5))
    }

    val udf_calc_periodic_value = udf(map_calc_periodic_value, periodic_value_schema)

    bonds_cast_df.withColumn("periodic_value", udf_calc_periodic_value(bonds_cast_df["value"], bonds_cast_df["coupon"],
    bonds_cast_df["yield"], bonds_cast_df["type"],
    bonds_cast_df["duration"]))\
      .withColumn("periodic_value", explode("periodic_value"))\
    .select("id", "value", "periodic_value.period", "periodic_value.cp", "periodic_value.pv", "periodic_value.aggpv",
      "periodic_value.quote")\
    .createOrReplaceTempView("periodic_value_table")

    spark.sql("select id as `Bond ID`, period as `Period`, cp as `Coupon payment`, pv as `PV of periodic payments`, "
    "aggpv as A from periodic_value_table").show(50)

    spark.sql("select id as `Bond ID`, aggpv as A, value as `FV`, quote as `Quote` "
    "from periodic_value_table where NOT aggpv = '0.00000'").show()


    spark.stop()
  }


  def calc_periodic_value(vvalue:DecimalType, coupon:DecimalType, vyield:DecimalType, vtype:StringType,
                          duration:IntegerType) = {
    var sList = List()
    var type_val:Int = 0

    if (types.equals("A")) {
      type_val = 1
    } else if (types.equals("S")) {
      type_val = 2
    } else if (types.equals("Q")) {
      type_val = 4
    } else if (types.equals("M")) {
      type_val = 12
    }
    print("type_val:" + type_val + "\n")

    val c = vvalue*((coupon*100)/type_val)
    var tpv = 0

    for (x <- 0 to Int(duration)) {
      var aList = List()

      if ((x+1) == duration) {
        val pv = (vvalue / pow(1 + ((vyield*100)/type_val), x+1)) + (c / pow(1 + ((vyield*100)/type_val), x+1))
        tpv += pv
        aList:+ (x+1)
        aList:+ c
        aList:+ pv
        aList:+ tpv
        aList:+ (tpv/vvalue)
      } else {
        val pv = (vvalue / pow(1 + ((vyield*100)/type_val), x+1)) + (c / pow(1 + ((vyield*100)/type_val), x+1))
        tpv += pv
        aList:+ (x+1)
        aList:+ (c)
        aList:+ (pv)
        aList:+ BigDecimal(0)
        aList:+ BigDecimal(0)
      }

      sList:+ aList
    }

    return sList
  }
}