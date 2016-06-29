import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jcdvorchak on 6/28/2016.
  *
spark-submit.sh --vcap ../properties/vcap.json --name "Sales by Weather" --class SalesByWeather --master https://169.54.219.20:8443 salesbyweather-1.0.jar
  */
object SalesByWeather {
  case class WeatherReadings(location: String, date: String, obs_val: Double)
  case class StoreSales(location: String, dept: String, date: String, sales: String)
  val jdbcUrl = "jdbc:db2://dashdb-entry-yp-dal09-07.services.dal.bluemix.net:50000/BLUDB:user=dash5611;password=ZKPU5GJ1f9Bq;"

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.println("Usage: SalesByWeather <outputPath>")
    }
//    val jdbcUrl = args(0)
    val outputPath = args(0) //"swift://spark-output.spark/completedailysales")

    val sparkConf = new SparkConf().setAppName("SalesByWeather")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)

    import sqlContext.implicits._

    val stores = sqlContext.load("jdbc", Map("url" -> jdbcUrl, "dbtable" -> "dash5611.STORES"))
    val dailySales = sqlContext.load("jdbc", Map("url" -> jdbcUrl, "dbtable" -> "dash5611.DAILY_SALES"))
    stores.registerTempTable("stores")
    dailySales.registerTempTable("daily_sales")

    val stations = sqlContext.load("jdbc", Map("url" -> jdbcUrl, "dbtable" -> "dash5611.GHCND_US_STATIONS"))
    val readings = sqlContext.load("jdbc", Map("url" -> jdbcUrl, "dbtable" -> "dash5611.US_TEMP"))
    stations.registerTempTable("stations")
    readings.registerTempTable("readings")


    var storeSales = sqlContext.sql(
      """
        SELECT s.LOCATION, d.DEPT, d.DATE, d.SALES
        FROM daily_sales d
        JOIN stores s
        ON d.STORE_ID = s.STORE""")

    storeSales = storeSales.map ( r =>
      StoreSales(r(0).toString, r(1).toString, r(2).toString.replaceAll("-", ""), r(3).toString)
    ).toDF

    val weatherReadings = sqlContext.sql(
      """
        SELECT s.STATE, s.NAME, r.DATE, r.OBS_TYPE, OBS_VALUE
        FROM readings r
        JOIN stations s
        ON r.STATION_ID = s.STATION_ID""")

    var avgWeatherReadings = weatherReadings.groupBy("STATE", "NAME", "DATE").avg("OBS_VALUE")
    avgWeatherReadings = avgWeatherReadings.map ( r =>
      WeatherReadings(r(0) + " " + r(1), r(2).toString, r(3).asInstanceOf[Double])
    ).toDF

    storeSales.registerTempTable("store_sales")
    avgWeatherReadings.registerTempTable("weather_readings")

    val combined = sqlContext.sql(
      """
        SELECT s.location, s.date, s.dept, s.sales, w.obs_val
        FROM store_sales s
        JOIN weather_readings w
        ON w.location = s.location AND w.date = s.date""")


    val locationAndDeptKeyVal = combined.rdd.map(r => (r(0)+","+r(2),r(1)+","+r(3)+","+r(4)))
    val locationKeyVal = combined.rdd.map(r => (r(0),r(1)+","+r(3)+","+r(4)))

    val locationAndDept = locationAndDeptKeyVal.groupByKey.map{pair =>
      var acount, atotal, bcount, btotal, ccount, ctotal, dcount, dtotal, ecount, etotal = 0.0
      var fcount, ftotal, gcount, gtotal, hcount, htotal, icount, itotal, jcount, jtotal = 0.0
      var kcount, ktotal, lcount, ltotal = 0.0
      pair._2.foreach{r=>
        var temp = (r.split(",")(2).toDouble * 9/5 ) - 459.67
        var sales = r.split(",")(1).toDouble
        if (temp <= 0) {
          acount+=1
          atotal+=sales
        } else if (temp > 0 && temp <= 10) {
          bcount+=1
          btotal+=sales
        } else if (temp > 10 && temp < 20) {
          ccount+=1
          ctotal+=sales
        } else if (temp > 20 && temp < 30) {
          dcount+=1
          dtotal+=sales
        } else if (temp > 30 && temp < 40) {
          ecount+=1
          etotal+=sales
        } else if (temp > 40 && temp < 50) {
          fcount+=1
          ftotal+=sales
        } else if (temp > 50 && temp < 60) {
          gcount+=1
          gtotal+=sales
        } else if (temp > 60 && temp < 70) {
          hcount+=1
          htotal+=sales
        } else if (temp > 70 && temp < 80) {
          icount+=1
          itotal+=sales
        } else if (temp > 80 && temp < 90) {
          jcount+=1
          jtotal+=sales
        } else if (temp > 90 && temp < 100) {
          kcount+=1
          ktotal+=sales
        } else {
          lcount+=1
          ltotal+=sales
        }
      }

      (pair._1+","+(atotal/acount)+","+(btotal/bcount)+","+(ctotal/ccount)+","+(dtotal/dcount)
        +","+(etotal/ecount)+","+(ftotal/fcount)+","+(gtotal/gcount)+","+(htotal/hcount)+","+(itotal/icount)
        +","+(jtotal/jcount)+","+(ktotal/kcount)+","+(ltotal/lcount))
    }

    val location = locationKeyVal.groupByKey.map{pair =>
      var acount, atotal, bcount, btotal, ccount, ctotal, dcount, dtotal, ecount, etotal = 0.0
      var fcount, ftotal, gcount, gtotal, hcount, htotal, icount, itotal, jcount, jtotal = 0.0
      var kcount, ktotal, lcount, ltotal = 0.0
      pair._2.foreach{r=>
        var temp = (r.split(",")(2).toDouble * 9/5 ) - 459.67
        var sales = r.split(",")(1).toDouble
        if (temp <= 0) {
          acount+=1
          atotal+=sales
        } else if (temp > 0 && temp <= 10) {
          bcount+=1
          btotal+=sales
        } else if (temp > 10 && temp < 20) {
          ccount+=1
          ctotal+=sales
        } else if (temp > 20 && temp < 30) {
          dcount+=1
          dtotal+=sales
        } else if (temp > 30 && temp < 40) {
          ecount+=1
          etotal+=sales
        } else if (temp > 40 && temp < 50) {
          fcount+=1
          ftotal+=sales
        } else if (temp > 50 && temp < 60) {
          gcount+=1
          gtotal+=sales
        } else if (temp > 60 && temp < 70) {
          hcount+=1
          htotal+=sales
        } else if (temp > 70 && temp < 80) {
          icount+=1
          itotal+=sales
        } else if (temp > 80 && temp < 90) {
          jcount+=1
          jtotal+=sales
        } else if (temp > 90 && temp < 100) {
          kcount+=1
          ktotal+=sales
        } else {
          lcount+=1
          ltotal+=sales
        }
      }

      (pair._1+",ALL,"+(atotal/acount)+","+(btotal/bcount)+","+(ctotal/ccount)+","+(dtotal/dcount)
        +","+(etotal/ecount)+","+(ftotal/fcount)+","+(gtotal/gcount)+","+(htotal/hcount)+","+(itotal/icount)
        +","+(jtotal/jcount)+","+(ktotal/kcount)+","+(ltotal/lcount))
    }

    var graphData = locationAndDept.union(location)
    graphData.repartition(1).saveAsTextFile(outputPath)

    sc.stop()
  }
}
