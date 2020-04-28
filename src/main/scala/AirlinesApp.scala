import java.text.SimpleDateFormat

import context.Context._
import models.{
  Airline,
  AirlineDelay,
  AirlineStats,
  CancelledFlight,
  FlightsStats
}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{
  FloatType,
  StringType,
  StructField,
  StructType
}
import sparkSession.implicits._
import utils.DateUtils

object AirlinesApp extends App {
  // Load files with airline delay and cancellation data from 2009 to 2018

  val folderFiles = "/home/josels/devel-scala/examples/DATA_SPARK/FILES/"
  val colunmsNamesAirlinesDelay =
    List(
      "FL_DATE as flightDate",
      "OP_CARRIER as airlineCode",
      "OP_CARRIER_FL_NUM as flightNumber",
      "ORIGIN as origin",
      "DEST as destination",
      "DEP_DELAY as depDelay",
      "ARR_DELAY as arrDelay",
      "DEP_TIME as depTime",
      "CANCELLED as cancelled",
      "CANCELLATION_CODE as reasonForCancellation"
    )
  val colunmsNamesAirlines =
    List("IATA_CODE as airlineCode", "AIRLINE as airlineName")
  val colunmsNamesAirports =
    List("IATA_CODE as airportCode", "CITY as cityName")

  val customSchemaAirlinesDelay = StructType(
    Seq(
      StructField("FL_DATE", StringType, true),
      StructField("OP_CARRIER", StringType, true),
      StructField("OP_CARRIER_FL_NUM", StringType, true),
      StructField("ORIGIN", StringType, true),
      StructField("DEST", StringType, true),
      StructField("DEP_DELAY", FloatType, false),
      StructField("ARR_DELAY", FloatType, false),
      StructField("DEP_TIME", FloatType, false),
      StructField("CANCELLED", FloatType, false),
      StructField("CANCELLATION_CODE", StringType, true)
    )
  )
  val airlines =
    loadData(folderFiles + "airlines.csv", colunmsNamesAirlines: _*)
  airlines.show(10)

  val airports =
    loadData(folderFiles + "airports.csv", colunmsNamesAirports: _*)
  airports.show(10)

  val airlinesDelay =
    loadAirlineDelay(folderFiles + "2010.csv", colunmsNamesAirlinesDelay: _*)
  airlinesDelay.show(10)

  // Solución ejercicio 1
  delayedAirlines(airlinesDelay, Some("2010"))

  // Solución ejercicio 2
  destinations(airlinesDelay, "DCA")

  // Solución ejercicio 3
  flightInfo(airlinesDelay)

  // Solución ejercicio 4
  daysWithDelays(airlinesDelay)

  sparkSession.stop()

  def loadAirlineDelay(file: String, fields: String*): Dataset[AirlineDelay] = {
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(file)
      .selectExpr(fields: _*)
      .withColumn("depDelay", 'depDelay.cast(FloatType))
      .withColumn("arrDelay", 'arrDelay.cast(FloatType))
      .withColumn("depTime", 'depTime.cast(FloatType))
      .withColumn("cancelled", 'cancelled.cast(FloatType))
      .as[AirlineDelay]
  }

  def loadData(file: String, fields: String*): DataFrame = {
    sparkSession.read
      .format("csv")
      .option("header", "true")
      .load(file)
      .selectExpr(fields: _*)
  }

  //1. ¿Cuáles son las aerolíneas más cumplidas y las menos cumplidas de un año en especifico?
  //La respuesta debe incluir el nombre completo de la aerolínea, si no se envia el año debe calcular con
  //toda la información disponible.
  /**
    * Un vuelo se clasifica de la siguiente manera:
    * ARR_DELAY < 5 min --- On time
    * 5 > ARR_DELAY < 45min -- small Delay
    * ARR_DELAY > 45min large delay
    *
    * Calcule por cada aerolinea el número total de vuelos durante el año (en caso de no recibir el parametro de todos los años)
    * y el número de ontime flights, smallDelay flighst y largeDelay flights
    *
    * Orderne el resultado por largeDelayFlights, smallDelayFlightsy, ontimeFlights
    *
    * @param ds Dataset
    * @param year Year for searching
    */
  def delayedAirlines(ds: Dataset[AirlineDelay],
                      year: Option[String]): Dataset[AirlineStats] = {

    val airlineStats =
      ds.filter(airlineDelay => {
          year match {
            case Some(v) =>
              v.equals(
                DateUtils
                  .getYearFromDateString(airlineDelay.flightDate)
                  .getOrElse("")
              )
            case None => true
          }
        })
        .groupBy($"airlineCode")
        .agg(
          count($"airlineCode") as "totalFlights",
          count(when($"arrDelay".cast(FloatType) >= 45, 1)) as "largeDelayFlights",
          count(
            when(
              $"arrDelay".cast(FloatType) >= 5 && $"arrDelay"
                .cast(FloatType) < 45,
              1
            )
          ) as "smallDelayFlights",
          count(when($"arrDelay".cast(FloatType) < 5, 1)) as "onTimeFlights"
        )

    airlineStats.show(10)

    val airlineJoinairlineStats = airlines
      .join(
        airlineStats,
        airlines.col("airlineCode") ===
          airlineStats.col("airlineCode")
      )
      .select(
        $"airlineName",
        $"totalFlights",
        $"largeDelayFlights",
        $"smallDelayFlights",
        $"onTimeFlights"
      )
      .orderBy($"largeDelayFlights", $"smallDelayFlights", $"onTimeFlights")

    airlineJoinairlineStats.show(10)

    airlineJoinairlineStats.as[AirlineStats]
  }

  // 2. Dado un origen por ejemplo DCA (Washington), ¿Cuáles son destinos y cuantos vuelos presentan durante la mañana, tarde y noche?
  /**
    * Encuentre los destinos a partir de un origen, y de acuerdo a DEP_TIME clasifique el vuelo de la siguiente manera:
    * 00:00 y 8:00 - Morning
    * 8:01 y 16:00 - Afternoon
    * 16:01 y 23:59 - Night
    * @param ds Dataset
    * @param origin codigo de aeropuerto de origin
    * @return
    */
  def destinations(ds: Dataset[AirlineDelay],
                   origin: String): Dataset[FlightsStats] = {
    val destinations = ds
      .filter($"origin" === origin)
      .groupBy($"destination")
      .agg(
        count(when($"depTime" <= 800.0, 1)) as "morningFlights",
        count(
          when(
            $"depTime" > 800.0 &&
              $"depTime" <= 1600.0,
            1
          )
        ) as "afternoonFlights",
        count(when($"depTime" > 1600.0, 1)) as "nightFlights"
      )

    val airlinesJoinDestination = airports.join(
      destinations,
      airports.col("airportCode") === destinations.col("destination")
    )
    airlinesJoinDestination.show(10)
    airlinesJoinDestination.as[FlightsStats]
  }

  //3. Encuentre ¿Cuáles son los números de vuelo (top 20)  que han tenido más cancelaciones y sus causas?
  /**
    * Encuentre los vuelos más cancelados y cual es la causa mas frecuente
    * Un vuelo es cancelado si CANCELLED = 1
    * CANCELLATION_CODE A - Airline/Carrier; B - Weather; C - National Air System; D - Security
    *
    * @param ds
    */
  def flightInfo(ds: Dataset[AirlineDelay]): Dataset[CancelledFlight] = {
    val cancelledFlights =
      ds.filter(_.cancelled == 1)
        .groupBy(
          $"flightNumber" as "number",
          $"origin",
          $"destination",
          $"reasonForCancellation" as "cause"
        )
        .agg(count($"cancelled") as "cancelled")
        .orderBy($"flightNumber".desc)

    cancelledFlights.show(20)
    cancelledFlights
      .as[CancelledFlight]
  }

  //4. ¿Que dias se presentan más retrasos históricamente?
  /**
    * Encuentre que dia de la semana se presentan más demoras,
    * sólo tenga en cuenta los vuelos donde ARR_DELAY > 45min
    *
    * @param ds Dataset
    * @return Una lista con tuplas de la forma (DayOfTheWeek, NumberOfDelays) i.e.("Monday",12356)
    */
  def daysWithDelays(ds: Dataset[AirlineDelay]): List[(String, Long)] = {
    val days = ds
      .filter(flight => flight.arrDelay.exists(_ > 45))
      .map(
        flight => DateUtils.getDayOfWeekString(flight.flightDate).getOrElse("")
      )
      .withColumnRenamed("value", "weekDay")
      .filter('weekDay =!= "")
      .groupBy('weekDay)
      .agg(count('weekDay) as "amount")

    days.show()
    days.map(day => (day.getString(0), day.getLong(1))).collect().toList
  }
}
