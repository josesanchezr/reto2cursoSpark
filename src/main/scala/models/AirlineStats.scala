package models

final case class AirlineStats(airlineName: String,
                              totalFlights: Long,
                              largeDelayFlights: Long,
                              smallDelayFlights: Long,
                              onTimeFlights: Long)
