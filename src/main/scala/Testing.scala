import scala.collection.mutable
import scala.util.Random
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.time.{ZoneId, ZonedDateTime}
import java.util
import java.util.TimeZone

import com.cloudera.sparkts._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import scala.collection.parallel.mutable.ParArray

def errorKey = "INVALID_KEY"
val fileName = ("/pimco/adm/F_TRAN.csv")
def isValid(str: String): Option[String] = {
  str match {
    case "" => None
    case "-998" => None
    case "-999" => None
    case _ => Some(str)
  }
}

def zdtToTs(curDate: ZonedDateTime): Timestamp = {
  new Timestamp(curDate.toInstant.toEpochMilli)
}

def getTimestamp(ts: String): Timestamp = {
  // Manually set the time zone for SimpleDateFormat since otherwise it uses system time
  val date = new SimpleDateFormat("yyyyMMdd")
  date.setTimeZone(TimeZone.getTimeZone("+00:00"))
  val dateTime = zdtToTs(ZonedDateTime.ofInstant(date.parse(ts).toInstant, ZoneId.of("+00:00")))
  dateTime
}

def tradeDelim = "_@_"

def getFundKey(prdctAcct: String, tradeClass: String): String = {
  prdctAcct + tradeDelim + tradeClass
}

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
 */
def getStatsPerPeriod(startIdx: Int,
                      endIdx: Int,
                      windowLength: Int,
                      dtIndex: DateTimeIndex,
                      tsRdd: TimeSeriesRDD[String],
                      totalPerDay: Broadcast[collection.Map[String, Double]]): ParArray[RDD[Row]] = {
  val movingStatsPerFundPerDay =
    (startIdx until endIdx).toParArray.map {
      idx => {
        val curDate = dtIndex.dateTimeAtLoc(idx)

        // Take a windowLength view of the data
        val sliced = tsRdd.slice(curDate.minusDays(windowLength), curDate)

        sliced.map(kt => {
          val (m: Double, sd: Double) = {
            val v = kt._2.toArray
            var sum: Double = 0
            var diffSum: Double = 0
            var count: Double = 0
            var mean: Double = 0
            var stdDev: Double = 0
            val tradeClass = kt._1.last + ""

            val days = sliced.index.size

            for (day <- 0 until days) {
              val s: Double = getRatioPerDay(totalPerDay, sliced, v, day, tradeClass)

              // We only consider stats for days on which the account was traded
              if (!v(day).isNaN) {
                count += 1
                sum += s
              }
            }

            mean = sum / count

            for (day <- 0 until days) {
              val s: Double = getRatioPerDay(totalPerDay, sliced, v, day, tradeClass)

              // We only consider stats for days on which the account was traded
              if (!v(day).isNaN) {
                diffSum += Math.pow(s - mean, 2.0)
              }
            }

            stdDev = Math.sqrt(diffSum / (count - 1))
            if (count > 1) {
              (mean, stdDev)
            } else if (count > 0) {
              (mean, 0.0)
            } else {
              (0.0, 0.0)
            }
          }

          Row(zdtToTs(curDate), kt._1, m, sd)
        })
          .cache()
          .coalesce(1)
      }
    }
  movingStatsPerFundPerDay
}

def dfForModelClustering(sc: SparkContext, sqlContext: SQLContext): DataFrame = {
  val startDate = ZonedDateTime.parse("2014-01-03T12:30:40Z[GMT]")

  val numPeriods = 3
  val windowLength = 30

  val tradesPerFundPerDayMax = 5
  val fundsPerDayMax = 3

  val rowA = Row("12-02-01", "F1" + tradeDelim + "R", "R")

  val trades = mutable.Queue[Row](rowA)

  val rdd = sc.parallelize(trades, 100)

  val fields = Seq(StructField("timestamp", StringType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("TRADE_CLASS_KEY", StringType, true))

  val df = sqlContext.createDataFrame(rdd, StructType(fields))
  df
}

val df = dfForModelClustering(sc, sqlContext)


def testTS(sc: SparkContext, sqlContext: SQLContext) = {
  val startDate = ZonedDateTime.parse("2014-01-03T00:00:00-08:00")

  val numPeriods = 3
  val windowLength = 30

  val tradesPerFundPerDayMax = 5
  val fundsPerDayMax = 3

  val trades = mutable.Queue[Row]()

  for (j <- 0 until numPeriods) {
    // Number of days to generate data for
    val numFunds = Math.ceil(Math.random() * fundsPerDayMax).toInt
    for (i <- 0 until numFunds) {
      // The number of funds traded per day
      val fundSeed = new Random(System.nanoTime())
      trades += Row(new Timestamp(startDate.plusDays(j).toInstant.toEpochMilli),
        "F" + i.toInt + tradeDelim + "R",
        "R",
        Math.ceil(fundSeed.nextDouble() * tradesPerFundPerDayMax))

      trades += Row(new Timestamp(startDate.plusDays(j).toInstant.toEpochMilli),
        "F" + i.toInt + tradeDelim + "S",
        "S",
        Math.ceil(fundSeed.nextDouble() * tradesPerFundPerDayMax))
    }
  }

  val rdd = sc.parallelize(trades, 100)

  val fields = Seq(StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("TRADE_CLASS", StringType, true),
    StructField("ACCTS_PER_DAY", DoubleType, true))

  val df = sqlContext.createDataFrame(rdd, StructType(fields))
  val acctsPerFundDaily = df.cache()

  val totalPerDay: Broadcast[collection.Map[String, Double]] = sc.broadcast(
    acctsPerFundDaily.select($"timestamp", $"TRADE_CLASS_KEY", $"ACCTS_PER_DAY")
      .groupBy($"timestamp", $"TRADE_CLASS_KEY")
      .sum("ACCTS_PER_DAY")
      .map {
        case Row(ts: Timestamp, tradeClass: String, totalCount: Double) => (getTotalKey(tradeClass, ts), totalCount)
      }.collectAsMap())

  val dtIndex = DateTimeIndex.uniform(startDate, numPeriods, new DayFrequency(1))

  val tsRdd = TimeSeriesRDD.timeSeriesRDDFromObservations(dtIndex, acctsPerFundDaily, "timestamp", "FUND_KEY", "ACCTS_PER_DAY").cache()
  tsRdd.count()

  val movingStatsPerFundPerDay:RDD[Row] = (getStatsPerPeriod(0, numPeriods, windowLength, dtIndex, tsRdd, totalPerDay)
    .reduce((a,b) => a.union(b)))

  val statsFields = Seq(StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("MEAN", DoubleType, true),
    StructField("STD_DEV", DoubleType, true))

  val statsDf = sqlContext.createDataFrame(movingStatsPerFundPerDay, StructType(statsFields))

  // Join the two tables so that we can match price to the computed windowed statistics, then filter by those
  // transactions that exceed 2 standard deviations
  val joined = acctsPerFundDaily.join(statsDf, Seq("timestamp", "FUND_KEY"))

  val joinedFields = Seq(StructField("timestamp", TimestampType, true),
    StructField("FUND_KEY", StringType, true),
    StructField("TRADE_CLASS_KEY", StringType, true),
    StructField("ACCTS_PER_DAY", DoubleType, true),
    StructField("TOTAL", DoubleType, true),
    StructField("MEAN", DoubleType, true),
    StructField("STD_DEV", DoubleType, true),
    StructField("RATIO", DoubleType, true))

  val stdDevs = sqlContext.createDataFrame(joined.map {
    case Row(timestamp: Timestamp, fundKey: String, tradeClass: String, acctsPerDay: Double, mean: Double, std_dev: Double) => {
      val total = totalPerDay.value.get(getTotalKey(tradeClass, timestamp)).get
      // If no activity occurred, then the ratio is zero. If stdDev is zero, then there is no deviation from the mean,
      // and thus the percentage of deviation from the mean is also zero.
      val ratio = if (total > 0 && std_dev > 0) {
        ((acctsPerDay / total) - mean) / std_dev
      } else {
        0.0
      }

      Row(timestamp, fundKey, tradeClass, acctsPerDay, total, mean, std_dev, ratio)
    }
  }, StructType(joinedFields)).cache()
  stdDevs.checkpoint()

  val filtered = stdDevs.filter($"RATIO" >= 2).cache()

  val sorted = filtered.sort(desc("ACCTS_PER_DAY"))

  val joinDf =  filtered.join(df, Seq("timestamp", "FUND_KEY", "TRADE_CLASS_KEY")).distinct()

  joinDf.show()
}