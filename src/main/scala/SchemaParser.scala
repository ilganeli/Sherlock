import org.apache.spark.SparkContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, DataFrame, SQLContext}

def readFile(sc: SparkContext, sqlContext: SQLContext, fileName: String,
             delim: Char = ','): DataFrame = {
  val a = sc.textFile(fileName)

  val schema = StructType(a.first().split(delim).map(fieldName => StructField(fieldName, StringType, true)))

  val noHeader = a.mapPartitionsWithIndex(
    (i, iterator) =>
      if ((i == 0 && iterator.hasNext)) {
        iterator.next
        iterator
      } else {
        iterator
      }).map(_.split(delim)).filter(_.length == schema.length).map(p => Row.fromSeq(p))

  sqlContext.createDataFrame(noHeader, schema)
}

def convertType(dataType: String): (DataType, Boolean) = {
  val splitS = dataType.split(" ")
  val dt: DataType =
    if(splitS(0).contains("CHAR")) {
      StringType
    } else if (splitS(0).contains("TIMESTAMP")) {
      TimestampType
    } else if (splitS(0).contains("NUMBER")){
      DoubleType
    } else {
      StringType
    }

  val nullable = !dataType.endsWith("NOT NULL")
  (dt, nullable)
}

val df: DataFrame = readFile(sc, sqlContext, "/pimco/adm/schema.csv/schema.csv")
df.rdd.groupBy(_ (0)).map {
  case (table: String, fields: Iterable[Row]) =>
    fields.map {
      case Row(tableName: String, columnName: String,
            columnId: String, dataType: String) => {
        val (dataType:DataType, nullable: Boolean) = convertType(dataType)
        StructField(columnName, dataType, nullable)
      }
    }

    //TODO Convert to structfield
}
