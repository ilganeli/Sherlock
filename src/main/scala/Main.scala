import java.sql.Timestamp
import java.time.{Duration, ZonedDateTime}
import java.util

import com.cloudera.sparkts._
import org.apache.commons.lang.mutable.MutableBoolean
import org.apache.hadoop.conf._
import org.apache.hadoop.fs.{FileSystem, FsShell, Path}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.joda.time.{DateTimeZone, DateTime}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.collection.parallel.mutable.ParArray

// Key used to fill in when no account identifier is found
def errorKey = "INVALID_KEY"

// Custom delimeters to use when creating strings from data fields to ensure that splitting by comma/underscore doesn't
// create problems
def tradeDelim = "_@_"
def colDelim = "_~_"
def modelDelim = "_$_"

/** The following variables are datasets persisted from individual computational steps. Doing this saves time when
  * doing data exploration.
  */

// The joinDf is the original set of transactions filtered to only include transactions for fund/date/trade-class
// that were > 2STD_DEV outside the mean
val joinDfFilename = "/pimco/adm/outliers/joinDf.parquet"

// The acctSources table tracks the origin of the accountID used in all datasets since it is merged from multiple tables
val acctSourcesFilename = "/pimco/adm/outliers/acctSources.csv"

// The base path potential models derived with the iterative algorithm. Individual files are saved as
// BASE_PATH_MINMODELSIZE.txt
val potentialModelFilename = "/pimco/adm/outliers/modelRepeats.csv"
val modelMembersFilename = "/pimco/adm/outliers/modelMembers.csv"
val modelTradesFilename = "/pimco/adm/outliers/modelTrades.csv"

/**
 * Helper function to reject input data that is empty, -998, or -999
 * @param str The field to validate
 * @return An option which contains the value of the string if valid
 */
def isValid(str: String): Option[String] = {
  str match {
    case "" => None
    case "-998" => None
    case "-999" => None
    case _ => Some(str)
  }
}

/**
 * Because Spark does not natively support over-writing existing data on HDFS, the following simplifies repeatedly
 * writing new datasets.
 */
def saveAsText(sc: SparkContext, rdd: RDD[_], name: String) = {
  val config = new Configuration()
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

  val fs = FileSystem.get(config)

  val pathToUse = name

  fs.delete(new Path(pathToUse), true)
  rdd.saveAsTextFile(pathToUse)

  val shell = new FsShell(config)
  shell.run(Array("-chmod", "-R", "777", pathToUse))
}

def saveAsObject(sc: SparkContext, rdd: RDD[_], name: String) = {
  val config = new Configuration()
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

  val fs = FileSystem.get(config)
  var pathToUse = name

  fs.delete(new Path(pathToUse), true)
  rdd.saveAsObjectFile(pathToUse)

  val shell = new FsShell(config)
  shell.run(Array("-chmod", "-R", "777", pathToUse))
}


/**
 * Because Spark does not natively support over-writing existing data on HDFS, the following simplifies repeatedly
 * writing new datasets.
 */
def saveAsDf(sc: SparkContext, df: DataFrame, name: String, useBasePath: Boolean = true) = {
  val config = new Configuration()
  config.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
  config.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))

  val fs = FileSystem.get(config)

  val pathToUse = name

  fs.delete(new Path(pathToUse), true)
  df.write.save(pathToUse)

  val shell = new FsShell(config)
  shell.run(Array("-chmod", "-R", "777", pathToUse))
}

/**
 * Helper function to create a Spark SQL Timestamp from a ZonedDateTime object. Used when creating a DataFrame.
 * @param curDate - The date to convert
 * @return A Spark SQL Timestamp equivalent to the ZonedDateTime object
 */
def zdtToTs(curDate: ZonedDateTime): Timestamp = {
  new Timestamp(curDate.toInstant.toEpochMilli)
}

def zdtToSimple(curDate: ZonedDateTime): String = {
  curDate.toString.split("T")(0).replace("-", "")
}


/**
 * Create a unique identifier for a fund based on a combination of fields
 * @param prdctAcct - The fund identifier
 * @param tradeClass - The trade class
 * @return A string delimited by the "tradeDelim" that is a unique key for this fund.
 */
def getFundKey(prdctAcct: String, tradeClass: String): String = {
  prdctAcct + tradeDelim + tradeClass
}

/**
 * Perform a cascading merge on a set of identifiers for a "acount" to create a unique identifier
 * @param binNumber
 * @param extAcct
 * @param repAlias
 * @param clientAcct
 * @return Returns the first field found from [binNumber, extAcct, repAlias, clientAcct] or an errorKey if no valid
 *         data exists in any of those.
 */
def getAcctKey(binNumber: Option[String],
               extAcct: Option[String],
               repAlias: Option[String],
               clientAcct: Option[String]): String = {
  binNumber.getOrElse(
    extAcct.getOrElse(
      repAlias.getOrElse(
        clientAcct.getOrElse(errorKey))))
}

/**
 * Perform a cascading merge on a set of identifiers for a "acount" to create a unique identifier labeled with the
 * source from which this data came.
 * @param binNumber
 * @param extAcct
 * @param repAlias
 * @param clientAcct
 * @return Returns the first field found from [binNumber, extAcct, repAlias, clientAcct] or an errorKey if no valid
 *         data exists in any of those.
 */
def getAcctKeyLabeled(binNumber: Option[String],
                      extAcct: Option[String],
                      repAlias: Option[String],
                      clientAcct: Option[String]): String = {
  val source = {
    if (binNumber.isDefined) "bin_number"
    else if (extAcct.isDefined) "ext_acct_key"
    else if (repAlias.isDefined) "rep_alias_key"
    else if (clientAcct.isDefined) "client_acct_key"
    else "ERROR"
  }

  binNumber.getOrElse(
    extAcct.getOrElse(
      repAlias.getOrElse(
        clientAcct.getOrElse(errorKey)))) + "," + source
}

/**
 * Read a file from the file system, which includes a header, delimeted with the provided character (default 'comma')
 * and convert it to a DataFrame where each field is typed as a String.
 * @param sc - The SparkContext
 * @param sqlContext - The SQL Context
 * @param fileName - The file name
 * @param delim - Delimeter to use (default 'comma')
 * @return A Spark DataFrame representing the input data
 */
def readFile(sc: SparkContext, sqlContext: SQLContext, fileName: String,
             delim: String = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)"): DataFrame = {
  val a = sc.textFile(fileName)

  // Define the schema for the DataFrame by creating a StringType StructField for every field
  val schema = StructType(
    a.first().split(delim)
      .map(fieldName => StructField(fieldName.replace("\"","").trim(), StringType, true)))

  // Take all but the first row of the dataset and split each line by the provided delimeter and drop any rows
  // that don't match the schema.
  // Lastly, convert each line to a Row object to create a DataFrame.
  val noHeader = {
    a.mapPartitionsWithIndex(
      (i, iterator) =>
        if ((i == 0 && iterator.hasNext)) {
          iterator.next
          iterator
        } else {
          iterator
        })
      .map(_.split(delim))
      .map(s => s.map(f => f.replace("\"","").trim))
      .filter(_.length == schema.length)
      .map(p => Row.fromSeq(p))
  }

  sqlContext.createDataFrame(noHeader, schema)
}

/**
 * Read a file from the file system, which includes a header, delimeted with the provided character (default 'comma')
 * and convert it to a DataFrame where each field is typed as a String.
 * @param sc - The SparkContext
 * @param sqlContext - The SQL Context
 * @param fileName - The file name
 * @param delim - Delimeter to use (default 'comma')
 * @return A Spark DataFrame representing the input data
 */
def readFile(sc: SparkContext, schemaFile: String, sqlContext: SQLContext, fileName: String,
             delim: String = ",(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)"): DataFrame = {
  val a = sc.textFile(fileName)


  // Define the schema for the DataFrame by creating a StringType StructField for every field
  val schema = StructType(sc.textFile(schemaFile).first().split(delim)
      .map(fieldName => StructField(fieldName.replace("\"","").trim(), StringType, true)))

  // Take all but the first row of the dataset and split each line by the provided delimeter and drop any rows
  // that don't match the schema.
  // Lastly, convert each line to a Row object to create a DataFrame.
  val noHeader = {
    a.mapPartitionsWithIndex(
      (i, iterator) =>
        if ((i == 0 && iterator.hasNext)) {
          iterator.next
          iterator
        } else {
          iterator
        })
      .map(_.split(delim))
      .map(s => s.map(f => f.replace("\"","").trim))
      .filter(_.length == schema.length)
      .map(p => Row.fromSeq(p))
  }

  sqlContext.createDataFrame(noHeader, schema)
}


/**
 * Helper function to generate the sources for the account mapping done when ingesting data.
 * @param sc - Spark Context
 * @param sqlContext - SQL Context
 * @param fTranCsv - The FTRAN table
 */
def generateAcctSources(sc: SparkContext, sqlContext: SQLContext, fTranCsv: String): Unit = {
  // Import implicits to allow shorthand for column selection
  val a = sc.textFile(fTranCsv)

  // Extract the schema
  val schema = a.first().split('|')

  // Create a map from the schema so that we can do column lookup by name
  val fieldMap = new util.HashMap[String, Integer]
  for (i <- 0 until schema.length) {
    fieldMap.put(schema(i), i)
  }

  // Drop the first row from the dataset, split by '|' and drop any rows that dont match the schema
  val noHeader: RDD[Array[String]] = a.mapPartitionsWithIndex(
    (i, iterator) =>
      if ((i == 0 && iterator.hasNext)) {
        iterator.next
        iterator
      } else {
        iterator
      })
    .map(_.split('|'))
    .filter(_.length == schema.length)

  // Process transaction data for funds
  // Get fund IDs
  val fieldsReal = Seq(StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("ACCT_KEY", StringType, true),
    StructField("TRADE_CLASS", StringType, true),
    StructField("SHARE_CLASS_KEY", StringType, true),
    StructField("EXECUTING_KEY", StringType, true),
    StructField("CLEARING_KEY", StringType, true),
    StructField("TRADING_ENTITY_KEY", StringType, true),
    StructField("FEED_KEY", StringType, true))

  val acctSources = noHeader.map { s =>
    val binNumber = isValid(s(fieldMap.get("bin_number")))
    val extAcct = isValid(s(fieldMap.get("ext_acct_key")))
    val repAlias = isValid(s(fieldMap.get("rep_alias_key")))
    val clientAcct = isValid(s(fieldMap.get("client_acct_key")))
    getAcctKeyLabeled(binNumber, extAcct, repAlias, clientAcct)
  }.distinct().cache()

  saveAsText(sc, acctSources.distinct().repartition(1), acctSourcesFilename)
}

/**
 * Ingest the F Tran dataset along with the various tables used to resolve keys to specific values. Then, filter the
 * input data to only consider fields we care about, and only those transactions that come from specific sources.
 * @param sc - The Spark Context
 * @param sqlContext - The SQL Context
 * @param fTranCsv - The fileName for the F TRAN dataset
 * @return A dataframe representing the selected columns of a filtered set of transactions from FTRAN.
 */
def createDataFrameFromFTran(sc: SparkContext, sqlContext: SQLContext, fTranCsv: String): DataFrame = {
  // Import implicits to allow shorthand for column selection
  import sqlContext.implicits._
  val a = sc.textFile(fTranCsv)

  // Extract the schema
  val schema = a.first().split('|')

  // Create a map from the schema so that we can do column lookup by name
  val fieldMap = new util.HashMap[String, Integer]
  for (i <- 0 until schema.length) {
    fieldMap.put(schema(i), i)
  }

  // Drop the first row from the dataset, split by '|' and drop any rows that dont match the schema
  val noHeader: RDD[Array[String]] = {
    a.mapPartitionsWithIndex(
      (i, iterator) =>
        if ((i == 0 && iterator.hasNext)) {
          iterator.next
          iterator
        } else {
          iterator
        })
      .map(_.split('|'))
      .filter(_.length == schema.length)
  }

  // Get table to resolve trade class code
  val tradeClasses = readFile(sc, sqlContext, "/pimco/adm/D_TRADE_CLASS.csv", '|')
  val tradeClassMap = tradeClasses.select($"trade_class_key", $"trade_class_code").map {
    case Row(key: String, code: String) => (key, code)
  }.collectAsMap()

  // Get table to resolve trust key
  val acct = readFile(sc, sqlContext, "/pimco/adm/D_ACCT.csv", '|')
  val acctTrustMap = acct.select($"acct_key", $"trust_key").map {
    case Row(acctKey: String, trustKey: String) => (acctKey, trustKey)
  }.collectAsMap()

  // Get table to resolve trust code
  val trust = readFile(sc, sqlContext, "/pimco/adm/D_TRUST.csv", '|')
  val trustCodeMap = trust.select($"trust_key", $"trust_code").map {
    case Row(key: String, code: String) => (key, code)
  }.collectAsMap()

  // Get table to resolve feed code
  val sdmFeed = readFile(sc, sqlContext, "/pimco/adm/D_SDM_FEED.csv", '|')
  val feedCodeMap = sdmFeed.select(sdmFeed("sdm_feed_key"), sdmFeed("feed_code")).map {
    case Row(key: String, code: String) => (key, code)
  }.collectAsMap()

  // Get table to resolve feed desc
  val feedDescMap = sdmFeed.select(sdmFeed("sdm_feed_key"), sdmFeed("feed_desc")).map {
    case Row(key: String, desc: String) => (key, desc)
  }.collectAsMap()

  // Get table to resolve feed location code
  val feedLocCodeMap = sdmFeed.select(sdmFeed("sdm_feed_key"), sdmFeed("feed_location_code")).map {
    case Row(key: String, code: String) => (key, code)
  }.collectAsMap()

  // Define the schema for the data frame we are creating
  val fieldsReal = Seq(StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("ACCT_KEY", StringType, true),
    StructField("TRADE_CLASS", StringType, true),
    StructField("SHARE_CLASS_KEY", StringType, true),
    StructField("EXECUTING_KEY", StringType, true),
    StructField("CLEARING_KEY", StringType, true),
    StructField("TRADING_ENTITY_KEY", StringType, true),
    StructField("USD_GROSS_AMT", DoubleType, true),
    StructField("FEED_KEY", StringType, true))

  // Process the data, filter to only consider valid transactions and those transactions that come from specific sources
  // NOTE: We use a flatMap instead of a map here because it allows us to easily filter out invalid data.
  // FlatMap will automatically skip empty returns so our return can then be either a Seq object containing a single
  // entry or an empty Seq() if the data is invalid.
  val dataAsRows = noHeader.flatMap { s =>
    //TODO Replace with actual field names
    val timestamp = s(fieldMap.get("trade_date_key"))
    val prdctAcctKey = s(fieldMap.get("prdct_acct_key"))
    val tradeClass = tradeClassMap.get(s(fieldMap.get("trade_class_key"))).get
    val shareClassKey = s(fieldMap.get("shareclass_cusip_key"))

    val binNumber = isValid(s(fieldMap.get("bin_number")))
    val extAcctKey = isValid(s(fieldMap.get("ext_acct_key")))
    val repAliasKey = isValid(s(fieldMap.get("rep_alias_key")))
    val clientAcctKey = isValid(s(fieldMap.get("client_acct_key")))

    val sdmFeedKey: String = s(fieldMap.get("sdm_feed_key"))
    val trustCode: String = trustCodeMap.get(acctTrustMap.get(prdctAcctKey).get).get

    // Only consider transactions that are either a Subscription or Redemption
    val checkTrade = tradeClass.equals("S") || tradeClass.equals("R")

    // Reject transactions from ALBRIDGE, BROADRIDGE, and ETF
    val checkFeedCode =
      !feedCodeMap.get(sdmFeedKey).get.equals("ALBRIDGE") &&
        !feedCodeMap.get(sdmFeedKey).get.equals("BROADRIDGE") &&
        !feedCodeMap.get(sdmFeedKey).get.equals("ETF")

    // Reject feeds from BrightScope or LEGACY
    val checkFeedDesc = !feedDescMap.get(sdmFeedKey).get.equals("BrightScope") &&
      !feedDescMap.get(sdmFeedKey).get.equals("LEGACY")

    // Reject NPB
    val checkFeedLocCode = !feedLocCodeMap.get(sdmFeedKey).get.equals("NPB")

    // Confirm the trustCode is either PIMS or PIMCOEQS
    val checkTrust = trustCode.equals("PIMS") || trustCode.equals("PIMCOEQS")

    // Create a Sequence of Rows to easily filter invalid rows
    if (checkTrade && checkFeedCode && checkFeedDesc && checkFeedLocCode && checkTrust) {
      Seq(Row(
        getTimestamp(timestamp),
        getFundKey(prdctAcctKey, tradeClass),
        getAcctKey(binNumber, extAcctKey, repAliasKey, clientAcctKey),
        tradeClass,
        shareClassKey,
        s(fieldMap.get("exec_reltn_key")),
        s(fieldMap.get("clearing_reltn_key")),
        s(fieldMap.get("trading_entity_key")),
        s(fieldMap.get("usd_gross_amt")).toDouble,
        sdmFeedKey))
    } else {
      Seq()
    }
  }

  // Create a dataframe
  sqlContext.createDataFrame(dataAsRows, StructType(fieldsReal))
}

/**
 * For a given day and vector of trades for a fund, get the ratio : TRADES_PER_FUND_PER_DAY : COUNT_ALL_TRADES_PER_DAY
 */
def getRatioPerDay(totalPerDay: Broadcast[collection.Map[Timestamp, Double]],
                   sliced: TimeSeriesRDD[String],
                   v: Array[Double],
                   day: Int): Double = {
  val ts: Timestamp = zdtToTs(sliced.index.dateTimeAtLoc(day))
  val totalCount = totalPerDay.value.getOrElse(ts, Double.NaN)
  val s = v(day) / totalCount
  s
}
/**
 * Helper function for creating a key to store the total transaction count per fund per day
 */
def getTotalKey(tradeClass: String, ts: Timestamp): String = {
  ts.toString + tradeDelim + tradeClass
}

/**
 * For a given day and vector of trades for a fund, get the ratio : TRADES_PER_FUND_PER_DAY : COUNT_ALL_TRADES_PER_DAY
 */
def getRatioPerDay(totalPerDay: Broadcast[collection.Map[String, Double]],
                   sliced: TimeSeriesRDD[String],
                   v: Array[Double],
                   day: Int,
                   tradeClass: String): Double = {
  val ts: Timestamp = zdtToTs(sliced.index.dateTimeAtLoc(day))
  val totalCount = totalPerDay.value.get(getTotalKey(tradeClass, ts))
  totalCount match {
    case Some(count) =>
      if (v(day).isNaN) {
        0.0
      } else {
        v(day) / count
      }
    case None => 0.0
  }
}

/**
 * For every fund on days where it was traded, calculate the moving average and moving standard deviation for the ratio:
 * TRADES_PER_FUND_PER_DAY : COUNT_ALL_TRADES_PER_DAY
 * @param startIdx - The index in the time series to scan from, this essentially maps to a start date in time
 * @param endIdx - The last index in the time series, this maps to a end date in time
 * @param windowLength
 * @param dtIndex
 * @param tsRdd
 * @param totalPerDay
 * @return
 */
def getStatsPerPeriod(startIdx: Int,
                      endIdx: Int,
                      windowLength: Int,
                      dtIndex: DateTimeIndex,
                      tsRdd: TimeSeriesRDD[String],
                      totalPerDay: Broadcast[collection.Map[String, Double]]): ParArray[RDD[Row]] = {

  val movingStatsPerFundPerDay =
    (startIdx until endIdx).toParArray.map {
      // For a given range, conver to a parAray to allow parallel processing
      idx => {
        // For every day, compute stats for that day
        val curDate = dtIndex.dateTimeAtLoc(idx)

        /** Take a windowLength view of the data. A slice of the TimeSeriesRDD is itself a TimeSeriesRDD - a time indexed
          * view of the data of a specified length. In this case, we're looking "windowLength" days into the past.
          * The key for each row is the unique identifier (in this case a combination of FUND_KEY and TRADE_CLASS)
          * Each row is an array of length endIdx-startIdx, where each index of the array represents the value for
          * that day in the time series.
          */
        val sliced = tsRdd.slice(curDate.minusDays(windowLength), curDate)

        // For every day in the index, compute statistics
        sliced.map(kt => {
          val (m: Double, sd: Double) = {
            val vals = kt._2.toArray
            // Define aggregates
            var sum: Double = 0
            var diffSum: Double = 0
            var count: Double = 0
            var mean: Double = 0
            var stdDev: Double = 0
            val tradeClass = kt._1.last + ""
            val days = sliced.index.size
            // Compute the mean of the daily ratio of trades for the current fund to the total trades for this day
            for (day <- 0 until days) {
              val s: Double = getRatioPerDay(totalPerDay, sliced, vals, day, tradeClass)
              // We only consider stats for days on which the account was traded
              if (!vals(day).isNaN) {
                count += 1
                sum += s
              }
            }
            mean = sum / count
            // Compute the sample standard deviation for the ratio of trades for the current fund to the total trades
            // for this day
            for (day <- 0 until days) {
              val s: Double = getRatioPerDay(totalPerDay, sliced, vals, day, tradeClass)
              // We only consider stats for days on which the account was traded
              if (!vals(day).isNaN) {
                diffSum += Math.pow(s - mean, 2.0)
              }
            }
            stdDev = Math.sqrt(diffSum / (count - 1))
            // Handle divide by zero errors for sample standard deviation
            if (count > 1) {
              (mean, stdDev)
            } else if (count > 0) {
              (mean, 0.0)
            } else {
              (0.0, 0.0)
            }
          }
          // Convert back to a row so we can then create a DataFrame again
          Row(zdtToTs(curDate), kt._1, m, sd)
        })
        .cache() // Persist this dataset in memory
      .coalesce(1)
        // Reduce to a single partition since the resulting data is rather small and we ultimately want
        // to union these results together to get a dataset representing moving averages for the entire dataset. Union
        // performs best when there are fewer partitions.
      }
    }
  movingStatsPerFundPerDay
}

/**
 * Ingest the FTRAN file, compute statistics (moving average, daily totals, and std-dev for trades) on the historical
 * data, and filter to only contain those trades where the ratio of the number of accounts traded in a fund versus the
 * total traded on that day was more than 2 STD-DEVS outside the mean.
 *
 * NOTE: To run this function run ./spark-launch-no_gc due to stack size limitations in GC causing error
 *
 * @param sc - The Spark Context
 * @param sqlContext - The SQL context
 * @param fTranCsv - The path to the FTRAN table
 * @param startDate
 * @param endDate
 */
def identifyOutliers(sc: SparkContext, sqlContext: SQLContext, fTranCsv: String,
                     startDate: ZonedDateTime, endDate: ZonedDateTime, windowLength: Int): DataFrame = {
  import sqlContext.implicits._
  // Take one month of data initially starting on Dec 01

  // From a start date of 2013-01-01, take 3 years and 2 months worth of data and compute 90 day moving averages/std-dev
  val numDays = Duration.between(startDate, endDate).toDays.toInt

  val df = createDataFrameFromFTran(sc, sqlContext, fTranCsv)

  // For the sake of identifying outliers, we only care about the unique identifier for the fund and the associated
  // accounts. We can join with the original dataset later to recover the other fields.
  val fundAcct = df.select($"timestamp", $"TRADE_CLASS", $"FUND_KEY", $"ACCT_KEY")

  // Group by timestamp, fundKey, and tradeClassKey (which uniquely defines a fund). Then, for every fund, count the
  // number of distinct accounts and save as a new column "ACCTS_PER_DAY" with Double type
  val acctsPerFundDaily = {
    fundAcct.groupBy("timestamp", "FUND_KEY", "TRADE_CLASS")
      .agg(countDistinct("ACCT_KEY")
        .alias("ACCTS_PER_DAY")
        .cast(DoubleType)).cache()
  }

  /**
   * For every day, calculate the total number of accounts traded on that day in all funds and save this as broadcast
   * variable.
   *
   * NOTE 1: Subscriptions and Redemptions are counted seperately
   *
   * NOTE 2: Broadcast variables are a way to efficiently share data across Spark executors by only making a single
   * copy of this data per executor. This gives us an efficient mechanism for sharing lookup tables.
   */
  val addTotalKey = udf((timestamp: Timestamp, tradeClass: String) => getTotalKey(tradeClass, timestamp))

  val totalPerDay: Broadcast[collection.Map[String, Double]] = sc.broadcast(
    acctsPerFundDaily.select($"timestamp", $"TRADE_CLASS", $"ACCTS_PER_DAY")
      .groupBy($"timestamp", $"TRADE_CLASS") // We group by date and trade class to treat "S" and "R" differently
      .agg(sum("ACCTS_PER_DAY").alias("SUM")) // Take the sum per day
      .withColumn("TOTAL_KEY", addTotalKey($"timestamp", $"TRADE_CLASS")) // Map each row to a tuple consisting of (KEY, COUNT) and aggregate it as a map on the driver.
      .select("TOTAL_KEY","SUM")
      .map { case Row(key: String, sum: Double) => (key, sum) }
      .collectAsMap() // Collect as a map on the driver to create a broadcast variable
  )

  // Create a DateTimeIndex for the TimeSeriesRDD we're going to create from the provided start date, for a set number
  // of periods, with a daily frequency.
  val dtIndex = DateTimeIndex.uniform(startDate, numDays, new DayFrequency(1))

  // Create a time series RDD where each key is the FUND_KEY and the values are the accounts traded per day in that
  // fund
  val tsRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(
    dtIndex, acctsPerFundDaily, "timestamp", "FUND_KEY", "ACCTS_PER_DAY").cache()
  tsRdd.count()

  // For every day in the time series RDD, generate the moving average and then unify these results into a single RDD
  val movingStatsPerFundPerDay: RDD[Row] = {
    getStatsPerPeriod(0, numDays, windowLength, dtIndex, tsRdd, totalPerDay)
      .reduce((a, b) => a.union(b))
  }

  // Create a new schema so we can create a new DataFrame
  val statsFields = Seq(
    StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("MEAN", DoubleType, true),
    StructField("STD_DEV", DoubleType, true)
  )

  val statsDf = sqlContext.createDataFrame(movingStatsPerFundPerDay, StructType(statsFields))

  // Join the two tables so that we can match trades to the computed windowed statistics, then filter by those
  // transactions that exceed 2 standard deviations
  val joined = acctsPerFundDaily.join(statsDf, Seq("timestamp", "FUND_KEY"))

  // Create a new DataFrame that also includes the daily total, as well as the ratio to that total
  val addTotal = udf((timestamp: Timestamp, tradeClass: String) =>
    totalPerDay.value.get(getTotalKey(tradeClass, timestamp)).get)

  val addRatio = udf((acctsPerDay: Double, total: Double, std_dev: Double, mean: Double) => {
    if (total > 0 && std_dev > 0) {
      ((acctsPerDay / total) - mean) / std_dev
    } else {
      0.0
    }
  })

  val stdDevs: DataFrame = {
    joined
      .withColumn("TOTAL", addTotal($"timestamp", $"TRADE_CLASS"))
      .withColumn("RATIO", addRatio($"ACCTS_PER_DAY", $"TOTAL", $"STD_DEV", $"MEAN"))
      .cache()
  }

  // Select only those trades with a ratio greater than 2
  val filtered: DataFrame = stdDevs.filter($"RATIO" >= 2).cache()

  // Join with the original dataset to recover the other fields in the dataset
  val joinDf: DataFrame = {
    filtered
      .join(df, Seq("timestamp", "FUND_KEY", "TRADE_CLASS")).distinct().cache()
  }
  joinDf.show()
  // Save to file
  saveAsDf(sc, joinDf, joinDfFilename)

  joinDf
}

/**
 * ***************************************************************************
 * ***************************************************************************
 * ***************************************************************************
 *
 * The following section contains code to generate RDDs for quick testing of various algorithms
 *
 * ***************************************************************************
 * ***************************************************************************
 * ***************************************************************************
 */

/**
 * Create an RDD that can be run inside the clustering algorithm to test clustering
 * @param sc
 * @param sqlContext
 * @return
 */
def rddForModelClustering(sc: SparkContext, sqlContext: SQLContext): RDD[(String, Iterable[String])] = {
  val startDate = ZonedDateTime.parse("2014-01-03T12:30:40Z[-08:00]")

  val numPeriods = 3
  val windowLength = 30

  val tradesPerFundPerDayMax = 5
  val fundsPerDayMax = 3

  val trades = mutable.Queue[Row]()

  // Case 1, Ratio > 0.8, curMinusOverlap < minModelSize, modelMinusOverlap < minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A3", "A4", "A5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A3", "A4", "A5", "A6"))

  // Case 2, Ratio > 0.8, curMinusOverlap > minModelSize, modelMinusOverlap < minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("B1", "B2", "B3", "B4", "B5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("B1", "B2", "B3", "B4", "B5", "B6", "B7", "B8"))

  // Case 3, Ratio > 0.8, curMinusOverlap < minModelSize, modelMinusOverlap > minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("C0", "C1", "C2", "C3", "C4", "C5", "C6",
    "C7", "C8", "C9", "C10", "C11", "C12", "C13", "C14", "C15", "C16"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("C3", "C4", "C5", "C6", "C7", "C8", "C9",
    "C10", "C11", "C12", "C13", "C14", "C15", "C16", "C17", "C20"))

  // Case 4, Ratio > 0.8, curMinusOverlap > minModelSize, modelMinusOverlap > minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("D0", "D1", "D2", "D3", "D4", "D5", "D7", "D8",
    "D9", "D10", "D11", "D12", "D13", "D14", "D15", "D16"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("D3", "D4", "D5", "D7", "D8", "D9", "D10", "D11",
    "D12", "D13", "D14", "D15", "D16", "D17", "D18", "D19"))

  // Case 5, Ratio > 0.4, curMinusOverlap > minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("E3", "E4", "E5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("E4", "E5", "E7", "E8", "E9"))

  // Case 6, Ratio < 0.8, curMinusOverlap < minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("F3", "F4", "F5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("F4", "F5", "F7", "F8"))

  val rdd = sc.parallelize(trades, 100)
    .map(s => (s(0).toString, s(1).toString, s(2).toString, s(3).asInstanceOf[Seq[String]]))
    .map(s => (s._1 + colDelim + s._2 + colDelim + s._3, s._4.toIterable))
  rdd
}

/**
 * Create a test RDD that can be used to evaluate FP Growth
 * @param sc
 * @param sqlContext
 * @return
 */
def rddForFPGrowth(sc: SparkContext, sqlContext: SQLContext): RDD[(String, Iterable[String])] = {
  val startDate = ZonedDateTime.parse("2014-01-03T12:30:40Z[GMT-08:00]")

  val numPeriods = 3
  val windowLength = 30

  val tradesPerFundPerDayMax = 5
  val fundsPerDayMax = 3

  val trades = mutable.Queue[Row]()

  // Case 1, Ratio > 0.8, curMinusOverlap < minModelSize, modelMinusOverlap < minModelSize
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A3", "A4", "A5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A4", "A5", "A6"))
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A3", "A4", "A5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A3", "A4", "A5", "A6"))
  trades += Row("12-02-01", "F1" + tradeDelim + "R", "R", Seq("A2", "A3", "A4", "A5"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A3", "A4"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A4", "A5", "A6", "A7", "A8", "A9"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A4", "A5", "A6", "A9", "A10"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A1", "A2", "A9", "A10"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A4", "A5", "A6", "A9", "A10"))
  trades += Row("12-02-02", "F1" + tradeDelim + "R", "R", Seq("A9", "A2", "A4", "A10"))

  val rdd = sc.parallelize(trades, 100)
    .map(s => (s(0).toString, s(1).toString, s(2).toString, s(3).asInstanceOf[Seq[String]]))
    .map(s => (s._1 + colDelim + s._2 + colDelim + s._3, s._4.toIterable))
  rdd
}

/**
 * ***************************************************************************
 * ***************************************************************************
 * ***************************************************************************
 *
 * The following section contains experimental code to identify and extract models.
 *
 * ***************************************************************************
 * ***************************************************************************
 * ***************************************************************************
 *
 */
def highRatio = 0.8
def lowRatio = 0.2

case class Model(accts: mutable.Set[Any], modelId: Long)

case class Match(bestRatio: Double, bestIdx: Int, bestOverlap: Int)

case class Occurrence(timestamp: String, fundKey: String, tradeClass: String)

case class ModelMatch(modelId: Long, minModelSize: Long, candidateRatio: Double, overlapSize: Long)

case class Sequence(get: ListBuffer[String], lastTrade: MutableBoolean = new MutableBoolean(false))

def getDailyAggs(joinDf: DataFrame): RDD[(String, Iterable[Any])] = {
  // For a particular fund
  // Create an ordered set of ACCTS per day that represent a potential model as the accounts that traded on that day in
  // a fund that was outside the norm
  val dailyAggs = {
    (joinDf.select("timestamp", "FUND_KEY", "TRADE_CLASS", "ACCT_KEY")
     .map(s => (s(0).toString + colDelim + s(1).toString + colDelim + s(2).toString, s(3))) // Map to an RDD
     .groupBy(_._1) // Group by timestamp and fund key
     .map(s => (s._1, s._2.map(f => f._2)))) // Get rid of the timestamp and fund key in each groupedBy result
  }
  dailyAggs
}

/**
 * We define the following terms:
 * Model - A set of accounts that may appear repeatedly in the data
 * Candidate - A set of accounts that traded in a fund on a particular day to compare with current models
 * Overlap - The number of accounts that are both in Model and in Candidate === Model Intersect Candidate
 *
 * At a high level, the goal is to greedily construct a list of possible models and look for repeated occurrences of
 * these models in the data.
 *
 * For every unique fund identifier, for every day, we compare each candidate set of accounts traded versus the
 * current model list and find the best match. If the match is sufficiently close (Overlap/Candidate.size > 0.8 OR
 * Overlap/Model.size > 0.8) then we believe this to be a repeat of an existing model.
 *
 * We can also consider the number of accounts that did not overlap and create new models by subsetting either the
 * existing model, or the candidate model.
 **/
def findModels(sc: SparkContext, sqlContext: SQLContext, joinDfFilename: String, modelSizeList: List[Int], iterations: Int): Unit = {
  // All information available for date/fund/trade-class combinations that traded above 2x std-dev
  val joinDf = sqlContext.read.load(joinDfFilename)

  var processed = 0

  // For a particular fund
  // Create an ordered set of ACCTS per day that represent a potential model as the accounts that traded on that day in
  // a fund that was outside the norm
  val dailyAggs = getDailyAggs(joinDf)

  /** To run with fake data uncomment this and comment out the previous definition of daily aggs. **/
  // val dailyAggs = rddForModelClustering(sc, sqlContext)

  // Go in order of slowest day to largest. In this way we can build models from the ground up rather than trying to
  // look for subsets (which is a more difficult problem)
  val dailyActivity = dailyAggs.collect().sortBy(a => a._2.size)

  // For every known model, use a Stack to track the history of that model. This allows us a view over time to observe
  // changes in the model and to quickly access the most recent version of the model.

  val debug = false
  var modelCount = 0

  /**
   * Helper function to create a new ocurrence of a model based on the current key combination:
   * Key is timestamp_#_FUND_KEY_#_TRADE_CLASS_#_ACCT_KEY
   */
  def getOccurrence(curKey: String): Occurrence = {
    val keySplit = curKey.split(colDelim)
    val fundSplit = keySplit(1).split(tradeDelim)
    val fundKey = fundSplit(0)
    val tradeClass = fundSplit(1)
    val timestamp = keySplit(0)
    Occurrence(timestamp, fundKey, tradeClass)
  }

  /**
   * Iterate over every day in the dataset
   */
  def getModels(modelHistory: ListBuffer[mutable.Stack[Model]],
                modelOccurrences: util.HashMap[Long, ListBuffer[Occurrence]],
                minModelSize: Int): ListBuffer[mutable.Stack[Model]] = {
    dailyActivity.foreach(day => {
      // As we process, track the info related to the best possible match for both sub/super set
      // The daily activity
      val curKey = day._1 // Key is timestamp_#_FUND_KEY_#_TRADE_CLASS_#_ACCT_KEY
      val curActivity: mutable.Set[Any] = mutable.Set(day._2.toSeq: _*)

      /**
       * Look through the model history and find the index of the best match
       * @return
       */
      /**
       * Find the best match for the current activity in the existing set of mdoels
       */
      def findBestMatch(testSet: mutable.Set[Any]): Match = {
        var bestRatio = 0.0
        var bestIdx = 0
        var bestOverlap = 0
        modelHistory.map(s => s.top).zipWithIndex.toParArray.foreach(model => {
          // The potential model to match against
          val testModel = model._1
          val numOverlap = testSet.intersect(testModel.accts).size

          val ratioCur = numOverlap / testSet.size.toDouble
          val ratioTest = numOverlap / testModel.accts.size.toDouble
          val maxRatio = Math.max(ratioCur, ratioTest)

          if (maxRatio > bestRatio) {
            bestRatio = maxRatio
            bestIdx = model._2
            bestOverlap = numOverlap
          }
        })
        Match(bestRatio, bestIdx, bestOverlap)
      }

      def getNewModel(modelAccts: mutable.Set[Any]): Model = {
        val occurrence = getOccurrence(curKey)
        val newModel = Model(modelAccts, modelCount)

        val newList = new ListBuffer[Occurrence]()
        newList.append(occurrence)
        modelOccurrences.put(modelCount, newList)

        modelCount += 1
        newModel
      }

      def addNewModel(modelAccts: mutable.Set[Any], checkExistence: Boolean = true): Unit = {
        var addNew = false
        if (checkExistence) {
          if (debug) println("    Checking for existing model to match: " + modelAccts)
          val matched = findBestMatch(modelAccts)

          if (matched.bestRatio > highRatio) {
            // We have found a match with an existing model so just update that one

            if (debug) println("    Incrementing existing model " + modelHistory(matched.bestIdx).top)
            incrementModel(matched)
          } else {
            addNew = true
          }
        } else {
          addNew = true
        }

        if (addNew) {
          val newModel: Model = getNewModel(modelAccts)
          val newModelMinusDaily = new mutable.Stack[Model]()
          newModelMinusDaily.push(newModel)
          modelHistory += newModelMinusDaily
          if (debug)
            println("    Adding new model " + modelAccts)
        }
      }

      def reduceModel(matched: Match): Unit = {
        // Reduce to the intersection of the models
        val reduceTo = modelHistory(matched.bestIdx).top.accts.intersect(curActivity)
        if (reduceTo.size > minModelSize) {
          val newModel: Model = getNewModel(reduceTo)
          modelHistory(matched.bestIdx).push(newModel)
          if (debug) println("     Reducing to " + reduceTo)
        }
      }

      def incrementModel(matched: Match): Unit = {
        val occurrence = getOccurrence(curKey)

        val modelId = modelHistory(matched.bestIdx).top.modelId

        modelOccurrences.get(modelId).append(occurrence)
      }

      /**
       * Helper function to update the model history given that we believe the current activity to be a superset of an
       * existing model.
       * @return
       */
      def updateGoodMatch(bestMatch: Match, candidate: mutable.Set[Any]): Unit = {
        // TODO Possibly add handling for overlap < minModelSize
        val modelMinusOverlap = modelHistory(bestMatch.bestIdx).top.accts.size - bestMatch.bestOverlap
        val candidateMinusOverlap = candidate.size - bestMatch.bestOverlap

        if ((candidateMinusOverlap >= minModelSize) && (modelMinusOverlap < minModelSize)) {
          // Get those accounts that are in the candidate but not in the existing model
          val dailyDiffModel: mutable.Set[Any] = candidate &~ modelHistory(bestMatch.bestIdx).top.accts

          if (debug) {
            println("  Checking " + curActivity + ".\n  Incrementing count for " + modelHistory(bestMatch.bestIdx).top.accts
              + ".\n  Attempting to add " + dailyDiffModel)
          }
          addNewModel(dailyDiffModel)

          incrementModel(bestMatch)
        } else if ((modelMinusOverlap >= minModelSize) && (candidateMinusOverlap < minModelSize)) {
          // Get those accounts that are in the existing model but not in the candidate
          val modelDiffDaily: mutable.Set[Any] = modelHistory(bestMatch.bestIdx).top.accts &~ candidate

          if (debug) {
            println("  Checking " + curActivity + ".\n  Reducing " + modelHistory(bestMatch.bestIdx).top.accts
              + ".\n  Attempting to add " + modelDiffDaily)
          }

          // Reduce needs to happen first, otherwise we find overlap with the existing model
          reduceModel(bestMatch)
          addNewModel(modelDiffDaily)
        } else if ((candidateMinusOverlap >= minModelSize) && (modelMinusOverlap >= minModelSize)) {
          val dailyDiffModel: mutable.Set[Any] = candidate &~ modelHistory(bestMatch.bestIdx).top.accts
          val modelDiffDaily: mutable.Set[Any] = modelHistory(bestMatch.bestIdx).top.accts &~ candidate

          if (debug) {
            println("  Checking " + curActivity + ".\n  Reducing " + modelHistory(bestMatch.bestIdx).top.accts
              + ".\n  Attempting to add " + modelDiffDaily + ".\n  Attempting to add " + dailyDiffModel)
          }

          reduceModel(bestMatch)
          addNewModel(dailyDiffModel)
          addNewModel(modelDiffDaily)
        } else {
          if (debug) {
            println("  Incrementing count for match:" + modelHistory(bestMatch.bestIdx).top)
          }

          incrementModel(bestMatch)
        }
      }


      /**
       * Now iterate through the model and find the best match for the current set
       */
      if (debug) println("Finding best match for " + curActivity)

      val bestMatch = findBestMatch(curActivity)
      processed += 1

      if (processed % 100 == 0) {
        println("Processed " + processed)
      }

      /**
       * Lastly, make a decision and either update an existing model or create a new model
       */
      if (bestMatch.bestRatio > highRatio) {
        // If there is a substantial (> highRatio) amount of overlap between a particular day and a model, and the day
        // appears to be a superset of that model, either partition the larger model into new models, or increase the
        // association with the existing model
        updateGoodMatch(bestMatch, curActivity)

      } else if (bestMatch.bestRatio > lowRatio) {
        // If there is some overlap, possibly create a new model to refine subsequent iterations or do nothing since
        // we don't have a good enough match with existing models
        val curMinusOverlap = curActivity.size - bestMatch.bestOverlap
        if (curMinusOverlap >= minModelSize) {
          val diff = curActivity &~ modelHistory(bestMatch.bestIdx).top.accts
          addNewModel(diff)
        } else {
          // In this case do nothing since we're essentially concluding there's no better model that we can create
          // since there is already some overlap but that we don't have enough overlap with an existing model to treat it
          // as a repeat occurrence of that model.
        }
      } else {
        if (curActivity.size >= minModelSize) {
          addNewModel(curActivity, false) // We know not to do a further check
        }
      }
    })

    modelHistory
  }

  def writeModelsToDisk(modelList: List[(Int, RDD[Model], util.HashMap[Long, ListBuffer[Occurrence]])]): Unit = {
    // For each generated set of models, create a modelMembers and modelTrades RDD
    val modelInfo: List[(RDD[String], RDD[String])] = {
      modelList.map {
        case (minModelSize: Int, bestMatches: RDD[Model], modelOccurrences) => {
          // Convert the set to a comma seperated list of accounts
          val modelMembers: RDD[String] = {
            bestMatches.flatMap(s => {
              val accts = s.accts
              val id = s.modelId
              accts.map(f => id + "," + minModelSize + "," + f)
            })
          }

          val modelOccurrencesB = sc.broadcast(modelOccurrences)
          val modelTrades: RDD[String] = {
            bestMatches.flatMap(s => {
              val id = s.modelId
              val occurrences = modelOccurrencesB.value.get(id)
              occurrences.map(f => {
                id + "," +
                  minModelSize + "," +
                  f.timestamp.split(" ")(0).replace("-", "") + "," +
                  f.fundKey + "," +
                  f.tradeClass
              })
            })
          }.distinct()

          (modelMembers, modelTrades)
        }
      }
    }

    // Lastly, unify these into a single RDD
    val modelMembers = modelInfo.map(s => s._1).reduce((a, b) => a.union(b)).repartition(1)
    val modelTrades = modelInfo.map(s => s._2).reduce((a, b) => a.union(b)).repartition(1)

    saveAsText(sc, modelMembers, modelMembersFilename)
    saveAsText(sc, modelTrades, modelTradesFilename)
  }

  /**
   * Generate a list of models for each minModelSize provided. Then, unify these individual models and output them to
   * disk
   */
  val bestMatches: List[(Int, RDD[Model], util.HashMap[Long, ListBuffer[Occurrence]])] = modelSizeList.map(minModelSize => {
    var modelHistory: ListBuffer[mutable.Stack[Model]] = ListBuffer[mutable.Stack[Model]]()
    val modelOccurrences = new util.HashMap[Long, ListBuffer[Occurrence]]

    for (i <- 1 to iterations) {
      // Reset counts for each
      modelOccurrences.keySet().toArray.foreach(s => modelOccurrences.put(s.asInstanceOf[Long], new ListBuffer[Occurrence]))
      modelHistory = getModels(modelHistory, modelOccurrences, minModelSize)
    }

    // Convert model history to an RDD of the most recently updated version of the model
    // The history of this model still exists in the modelHistory list but is not used for the sake of evaluation.
    val repeatedModels = modelHistory.filter(s => modelOccurrences.get(s.top.modelId).length > 1)
    val bestMatches: RDD[Model] = sc.parallelize(repeatedModels.map(s => s.top).toSeq).cache()

    // Filter by those models with more than one occurrence
    bestMatches.count()

    (minModelSize, bestMatches, modelOccurrences)
  })

  // Write the models to disk
  writeModelsToDisk(bestMatches)
}

case class TestModel(timestamp: String, fundKey: String, tradeClass: String)

/**
 * Reformat the columns in the joinDF file to get rid of dashes in timestamps and trade class identifier in the fund
 * key to compare against saved model data
 */
def getJoinDfForMatch(sqlContext: SQLContext, joinDfFilename: String): DataFrame = {
  import sqlContext.implicits._
  val reformatTimestamp = udf((timestamp: Timestamp) => timestamp.toString.split(" ")(0).replace("-", ""))

  // Get rid of the trade class encoded in the fund key
  val reformatFundKey = udf((fundKey: String) => fundKey.split(tradeDelim)(0))

  val joinDf = {
    sqlContext.read.load(joinDfFilename)
    .withColumn("timestamp", reformatTimestamp($"timestamp")) // Reformat timestamp
    .withColumn("FUND_KEY", reformatFundKey($"FUND_KEY")) // Reformat fund key
  }

  joinDf
}
/**
 * Get an RDD representing the model members for each model from the model members file
 */
def getModelMembersRdd(sc: SparkContext, modelMembersFilename: String): RDD[(Long, Long, Set[String])] = {
  sc.textFile(modelMembersFilename)
    .map(s => s.split(",")) //ID, MinModelSize, Member
    .map(s => ((s(0), s(1)), s(2)))
    .groupByKey()
    .map(s => (s._1._1.toLong, s._1._2.toLong, s._2.toSet)) //ID, MinModelSize,  Members,
}

/**
 * Get a dataframe representing the trades associated with each detected model
 */
def getModelTradesDf(sc: SparkContext, sqlContext: SQLContext, modelTradesFilename: String): DataFrame = {
  import sqlContext.implicits._
  sc.textFile(modelTradesFilename)
      .map(s => s.split(","))
      .map(s => (s(0), s(1), s(2).replace("-",""), s(3), s(4))) // Get rid of dashes in timestamp and convert to tuple a
      .toDF("model_id", "model_size","timestamp","FUND_KEY", "TRADE_CLASS")
}

/**
 * Get a set of all accounts in a given modelMembers file
 */
def getAllAccountsSet(sc: SparkContext, modelMembersFilename: String): Set[String] = {
  sc.textFile(modelMembersFilename)
      .map(s => s.split(","))
      .map(s => s(2)) // Account
      .distinct()
    .collect()
    .toSet
}

/**
 * Helper function to generate a set of accounts associated with a particular trade represented
 * by testModel
 * @param testModelS - The unique identifier for a set of trades - FUND_KEY, CLASS, and timestamp
 * @param joinDf - The joinDF dataFrame
 */
def getTestModelAccounts(sqlContext: SQLContext, testModelS: TestModel, joinDf: DataFrame): Set[String] = {
  import sqlContext.implicits._
  val ts = testModelS.timestamp
  val fundKey = testModelS.fundKey
  val tc = testModelS.tradeClass
  val testModelAccts = {
    joinDf.filter($"timestamp" === ts &&
      $"FUND_KEY" === fundKey &&
      $"TRADE_CLASS" === tc).select("ACCT_KEY").distinct().map(s => s(0).toString)
      .collect
      .toSet
  }
  testModelAccts
}

def findMatchInDays(sc: SparkContext, sqlContext: SQLContext,
                    joinDfFilename: String, testModelS: TestModel): ListBuffer[Occurrence] = {

  // All information available for date/fund/trade-class combinations that traded above 2x std-dev

  // Remove dashes in timestamp and convert from zoned date time to a simple date time string
  val joinDf = getJoinDfForMatch(sqlContext, joinDfFilename)

  val dailyAggs: RDD[(String, Iterable[Any])] = getDailyAggs(joinDf)

  val testModelAccts = getTestModelAccounts(sqlContext, testModelS, joinDf)

  /** To run with fake data uncomment this and comment out the previous definition of daily aggs. **/
  // val dailyAggs = rddForModelClustering(sc, sqlContext)

  // Go in order of slowest day to largest. In this way we can build models from the ground up rather than trying to
  // look for subsets (which is a more difficult problem)
  val dailyActivity = dailyAggs.collect().sortBy(a => a._2.size)

  // For every known model, use a Stack to track the history of that model. This allows us a view over time to observe
  // changes in the model and to quickly access the most recent version of the model.
  val debug = false
  val modelOccurrences = new ListBuffer[Occurrence]

  /**
   * Helper function to create a new ocurrence of a model based on the current key combination:
   * Key is timestamp_#_FUND_KEY_#_TRADE_CLASS_#_ACCT_KEY
   */
  def getOccurrence(curKey: String): Occurrence = {
    val keySplit = curKey.split(colDelim)
    val fundKey = keySplit(1)
    val tradeClass = keySplit(2)
    val timestamp = keySplit(0)
    Occurrence(timestamp, fundKey, tradeClass)
  }

  /**
   * Iterate over every day in the dataset
   */
  def findMatches() = {
    dailyActivity.foreach(day => {
      // As we process, track the info related to the best possible match for both sub/super set
      // The daily activity
      val curKey = day._1 // Key is timestamp_#_FUND_KEY_#_TRADE_CLASS_#_ACCT_KEY
      val candidateSet: mutable.Set[String] = mutable.Set(day._2.toSeq.asInstanceOf[Seq[String]]: _*)

      val numOverlap = testModelAccts.intersect(candidateSet).size

      //val ratioCandidate = numOverlap / candidateSet.size.toDouble
      val ratioTest = numOverlap / testModelAccts.size.toDouble
      //val maxRatio = Math.max(ratioCandidate, ratioTest)

      // Find any time we had a good match
      if (ratioTest > 0.8) {
        modelOccurrences.append(getOccurrence(curKey))
      }
    })
  }

  findMatches()
  modelOccurrences
}

/** Check a specific model against a known set of models to check for overlap **/
def findMatchInModels(sc: SparkContext, sqlContext: SQLContext,
                      joinDfFilename: String, testModelS: TestModel, modelMembersFilename: String): (ListBuffer[ModelMatch], String) = {
  val joinDf = getJoinDfForMatch(sqlContext, joinDfFilename)

  val dailyAggs: RDD[(String, Iterable[Any])] = getDailyAggs(joinDf)

  val testModelAccts = getTestModelAccounts(sqlContext, testModelS, joinDf)

  val modelMatches = new ListBuffer[ModelMatch] // ModelIdx, BestMatch

  // Ingest the model members file
  val modelMembers = getModelMembersRdd(sc, modelMembersFilename)

  // Construct a list of overlaps with existing models
  modelMembers.foreach(testModel => {
    val modelId = testModel._1
    val modelSize = testModel._2
    val accounts = testModel._3

    val numOverlap = testModelAccts.intersect(accounts).size

    val ratioCandidate = numOverlap / accounts.size.toDouble

    if (numOverlap >= modelSize) {
      modelMatches.append(ModelMatch(modelId, modelSize, ratioCandidate, numOverlap))
    }
  })

  // / Get the set of all accounts in all models and then check how many accounts in the testModel are not
  // represented by any model
  val allAccounts = getAllAccountsSet(sc, modelMembersFilename)

  val notInModels = testModelAccts &~ allAccounts
  (modelMatches, (100 * notInModels.size / testModelAccts.size.toDouble) + "%")
}

/**
 * To facilitate analysis of modeling output, it's useful to generate a couple of summary tables that aggregate
 * stats on the number of accounts traded for each model per day and the USD sum for each of these transactions.
 */
def generateTradeAnalysis_TE(sc: SparkContext, sqlContext: SQLContext,
                             joinDfFilename: String, modelTradesFilename: String): Unit = {
  import sqlContext.implicits._

  val joinDf = getJoinDfForMatch(sqlContext, joinDfFilename)

  // Ingest the model trades file and create a dataframe
  val modelTrades = getModelTradesDf(sc, sqlContext, modelTradesFilename)

  // Join the two tables so that we have the model ID alongside the original transactions
  val withModel = {
    joinDf.join(modelTrades, Seq("timestamp", "FUND_KEY", "TRADE_CLASS"))
      .select(modelTrades("model_id"), modelTrades("model_size"), joinDf("*")).cache()
  }

  val groupedFirm = withModel.groupBy("timestamp", "FUND_KEY", "TRADE_CLASS", "EXECUTING_KEY", "CLEARING_KEY")
  val groupedTE = withModel.groupBy("timestamp", "FUND_KEY", "TRADE_CLASS", "TRADING_ENTITY_KEY")

  // Get the number of distinct accounts per day
  val acctsPerDayFirm = {
    groupedFirm.agg(countDistinct("ACCT_KEY")
      .alias("ACCTS_PER_DAY")
      .cast(DoubleType))
  }

  // Get the total spend per day
  val sumUsdFirm = {
    groupedFirm.agg(sum("USD_GROSS_AMT")
      .alias("SUM_USD_GROSS_AMT")
      .cast(DoubleType))
  }

  val acctsPerDayTE = {
    groupedTE.agg(countDistinct("ACCT_KEY")
      .alias("ACCTS_PER_DAY")
      .cast(DoubleType))
  }

  // Get the total spend per day
  val sumUsdTE = {
    groupedTE.agg(sum("USD_GROSS_AMT")
      .alias("SUM_USD_GROSS_AMT")
      .cast(DoubleType))
  }

  // Join the full table to add these additional two columns
  val joinedTE = {
    withModel
      .join(acctsPerDayTE, Seq("timestamp", "FUND_KEY", "TRADE_CLASS", "TRADING_ENTITY_KEY"))
      .join(sumUsdTE, Seq("timestamp", "FUND_KEY", "TRADE_CLASS", "TRADING_ENTITY_KEY"))
      .select($"model_id", $"model_size", acctsPerDayTE("*"), sumUsdTE("SUM_USD_GROSS_AMT")).distinct()
  }

  val joinedFirm = {
    withModel
      .join(acctsPerDayFirm, Seq("timestamp", "FUND_KEY", "TRADE_CLASS", "EXECUTING_KEY", "CLEARING_KEY"))
      .join(sumUsdFirm, Seq("timestamp", "FUND_KEY", "TRADE_CLASS", "EXECUTING_KEY", "CLEARING_KEY"))
      .select($"model_id", $"model_size", acctsPerDayFirm("*"), sumUsdFirm("SUM_USD_GROSS_AMT")).distinct()
  }

  // Save the output to files
  val tradeAnalysisTEFilename = "/pimco/adm/outliers/tradeAnalysisTE.csv"
  val tradeAnalysisFirmFilename = "/pimco/adm/outliers/tradeAnalysisFirm.csv"

  saveAsText(sc, joinedFirm.rdd.map(s => {
    val sum: Double = s(8).asInstanceOf[Double]
    (s(0) + "," + //model_id
      s(1) + "," + //model_size
      s(2) + "," + // timestamp
      s(3) + "," + // fund_key
      s(4) + "," + // trade_class
      s(5) + "," + // exec_key
      s(6) + "," + // clearing_key
      s(7) + "," + // accts_per_day
      f"$sum%.2f") // sum_usd_gross_amt
  }).repartition(1), tradeAnalysisFirmFilename)

  saveAsText(sc, joinedTE.rdd.map(s => {
    val sum = s(6).asInstanceOf[Double]
    (s(0) + "," + //model_id
      s(1) + "," + // model_size
      s(2) + "," + // timestamp
      s(3) + "," + // fund_key
      s(4) + "," + // trading_entity_key
      s(5) + "," + // accts_per_day
      f"$sum%.2f") // sum_usd_gross_amt
  }).repartition(1), tradeAnalysisTEFilename)
}

/**
 * An anylsis function to identify all sequences of consecutive daily trades
 */
def identifySequences(sc: SparkContext, sqlContext: SQLContext,
                      joinDfFilename: String,
                      modelMembersFilename: String,
                      modelTradesFilename: String): Unit = {
  import sqlContext.implicits._

  // Get all accounts traded on each fund per day
  val joinDf = getJoinDfForMatch(sqlContext, joinDfFilename)
    .select("timestamp", "FUND_KEY", "TRADE_CLASS", "ACCT_KEY").cache()

  joinDf.count()

  // For every model, generate a mapping for the accounts contained in that model to indexes from 0 to N
  val modelMembers = sc.broadcast(getModelMembersRdd(sc, modelMembersFilename)
    .map(s => {
      val id = s._1
      val size = s._2
      val accts = s._3
      val acctMap = accts.zipWithIndex.toMap[String, Int]

      // Next, for every trade of this model
      (id, acctMap)
    }).collectAsMap())

  // For every model, get the dates on which it trades
  val modelTradesDf = getModelTradesDf(sc, sqlContext, modelTradesFilename)
  val modelTradeDates = {
    modelTradesDf.select("model_id", "FUND_KEY", "TRADE_CLASS", "timestamp")
      .collect()
  }

  val modelDates = sc.broadcast(modelTradeDates.map(s => s(3)).toSet)
  val fundKeys = sc.broadcast(modelTradeDates.map(s => s(1)).toSet)
  // We can speed up this next step by pre-filtering joinDF to only include the FUND_KEY, and timestamp present in
  // modelTradeDates
  val filteredDf = joinDf.filter($"timestamp".isin(modelDates.value.toSeq.map(lit(_)): _*) &&
    $"FUND_KEY".isin(fundKeys.value.toSeq.map(lit(_)): _*)).select("timestamp", "FUND_KEY", "TRADE_CLASS", "ACCT_KEY").distinct().cache()
  filteredDf.count()

  // Next, for every modelId, filter joinDF to get all accounts that traded on a day, fund key, and trade class
  // matching the model. From this set of accounts, track which of the accounts in the model appeared on this day
  // Thus for every trade of the model, the output is an array representing the accounts in the model
  // that traded on that day
  val modelAccountVecs = modelTradeDates.toSeq.toParArray.map(s => {
    val id = s.getString(0).toInt
    val fundKey = s.getString(1)
    val tradeClass = s.getString(2)
    val timestamp = s.getString(3)

    val filtered = filteredDf.filter($"timestamp" === timestamp &&
      $"FUND_KEY" === fundKey &&
      $"TRADE_CLASS" === tradeClass)

    val accounts = filtered.select("ACCT_KEY").flatMap(s => {
      val dailyAcct = s.getString(0)
      val modelAccounts = modelMembers.value.get(id).get

      if (modelAccounts.contains(dailyAcct)) {
        Seq(modelAccounts.get(dailyAcct).get) // Get the ID associated with this account
      } else {
        Seq()
      }
    }).collect().toSeq

    (id, fundKey, tradeClass, timestamp, accounts)
  }).toArray.toSeq

  val modelIds = modelMembers.value.keySet

  // For every model, for every account in that model, generate a Set of sequences representing consecutive occurrences
  val sequences = modelIds.toSeq.toParArray.map(id => {
    /** ****
    Per MODEL
      * *****/
    val modelTrades = modelAccountVecs.filter(s => s._1 == id) // Filter the overall set to just the ids for this model
    val sortedTrades = modelTrades.sortBy(s => s._4) // Sort by time

    // The dataset representing the arrays of accounts that traded in this model per trade occurrence.
    val acctSets = sortedTrades.map(s => s._5)

    // Create an array of sequence sets and current sequences for each account to aggregate results across trades
    val modelAccounts = modelMembers.value.get(id).get
    val sequences: Array[mutable.Set[Sequence]] =
      modelAccounts.toArray.map(s => mutable.Set[Sequence]())

    val curSeqs: Array[Sequence] = modelAccounts.toArray.map(s => Sequence(ListBuffer[String]()))

    for (i <- acctSets.indices) {
      /** ****
      Per Trade
        * *****/
      val date = sortedTrades(i)._4
      val accountsPerDay = acctSets(i).toSet

      for (j <- curSeqs.indices) {
        /** ****
       Per Account
          * *****/
        val traded = accountsPerDay.contains(j)
        val sequenceSetAcct = sequences(j) // The set of sequences for this account
        val curSeqAcct = curSeqs(j)

        // If this account traded, add it to the existing sequence. Otherwise, this terminates a previous sequence,
        // so commit that and reset to a new sequence
        if (traded) {
          curSeqAcct.get += date
        } else {
          //Track when the sequence is the last trade to be traded
          if (i == acctSets.length - 1) {
            curSeqAcct.lastTrade.setValue(true)
          }

          if (curSeqAcct.get.size > 1) {
            sequenceSetAcct.add(curSeqAcct)
          } else if (curSeqAcct.lastTrade.isTrue && curSeqAcct.get.nonEmpty) {
            sequenceSetAcct.add(curSeqAcct)
          }

          // Start a new sequence
          curSeqs(j) = Sequence(ListBuffer[String]())
        }
      }
    }

    (id, sequences)
  })

  val flattened = sc.parallelize(sequences.flatMap(s => {
    val modelId = s._1
    val sequences = s._2
    val memberMap = modelMembers.value.get(modelId).get.map(_.swap)
    sequences.zipWithIndex.flatMap {
      case (seqSet: mutable.Set[Sequence], acctId: Int) => {
        val acct = memberMap.get(acctId).get
        val seqs = seqSet.map(seq => {
          (modelId, acct, seq.get.head, seq.get.last, seq.get.size, if (seq.lastTrade.isTrue) 1 else 0)
        })
        seqs
      }
    }
  }).toArray.toSeq)

  saveAsText(sc, flattened.map(s => s._1 + "," +
    s._2 + "," +
    s._3 + "," +
    s._4 + "," +
    s._5 + "," +
    s._6).repartition(1), "/pimco/adm/outliers/sequenceAnalysis.csv")
}

/** Use the FP growth algorithm to find frequently repeated sets in the data **/
def runFPGrowth(sc: SparkContext, sqlContext: SQLContext, joinDfFilename: String, dataWithRatioName: String, minModelSize: Int): Unit = {
  import org.apache.spark.mllib.fpm.FPGrowth
  import sqlContext.implicits._
  // All information available for date/fund/trade-class combinations that traded above 2x std-dev
  val joinDf = sqlContext.read.load(joinDfFilename)
  var processed = 0
  // For a particular fund
  // Create an ordered set of ACCTS per day that represent a potential model as the accounts that traded on that day in
  // a fund that was outside the norm
  val dailyAggsFile = (joinDf.select("timestamp", "FUND_KEY", "TRADE_CLASS", "ACCT_KEY")
    .map(s => (s(0).toString + colDelim + s(1).toString + colDelim + s(2).toString, s(3))) // Map to an RDD
    .groupBy(_._1) // Group by timestamp and fund key
    .map(s => (s._1, s._2.map(f => f._2)))) // Get rid of the timestamp and fund key in each groupedBy result

  // val dailyAggs = rddForFPGrowth(sc, sqlContext)

  // Go in order of slowest day to largest. In this way we can build models from the ground up rather than trying to
  // look for subsets (which is a more difficult problem)
  val dailyActivity = dailyAggsFile.map(s => s._2.toArray.distinct).cache()

  val fpg = (new FPGrowth()
    .setMinSupport(0.1)
    .setNumPartitions(200))
  val model = fpg.run(dailyActivity)

  // model.freqItemsets.collect().foreach { itemset =>
  //   println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
  // }

  val asDf = model.freqItemsets.map(s => (s.items.length, s.freq)).toDF
}

// Parameters for the model identification algorithm
val startDate = ZonedDateTime.parse("2013-01-01T00:00:00-08:00")
val endDate = ZonedDateTime.parse("2016-02-01T00:00:00-08:00")
val windowLength = 90

// The HDFS path to the unfiltered file containing raw transactions
val fTranCsv = "/pimco/adm/F_TRAN.csv"

val testModelS = TestModel("20150716", "1008043", "S")
val modelSizeList = List(50, 100, 200)
val iterations = 5

// val joinDf = identifyOutliers(sc, sqlContext, fTranCsv, startDate, endDate, windowLength)

// findModels(sc, sqlContext, joinDfFilename, modelSizeList, iterations)
//
// generateTradeAnalysis_TE(sc, sqlContext, joinDfFilename, modelTradesFilename)
//
//testModel(sc, sqlContext, joinDfFilename, testModelS)
//
//val daysMatch = findMatchInDays(sc, sqlContext, joinDfFilename, testModelS)
//
//val modelsMatch = findMatchInModels(sc, sqlContext, joinDfFilename, testModelS, modelMembersFilename)
//
//identifySequences(sc, sqlContext, joinDfFilename, modelMembersFilename, modelTradesFilename)