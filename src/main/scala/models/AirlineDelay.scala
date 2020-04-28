package models

final case class AirlineDelay(flightDate: String,
                              airlineCode: String,
                              flightNumber: String,
                              origin: String,
                              destination: String,
                              depDelay: Option[Float],
                              arrDelay: Option[Float],
                              depTime: Option[Float],
                              cancelled: Float,
                              reasonForCancellation: Option[String])
