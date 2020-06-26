package cse512

object HotzoneUtils {


  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    val rectangleCoordinates = queryRectangle.split(',').map(_.toDouble) // array of doubles
    val a = rectangleCoordinates(0)
    val b = rectangleCoordinates(1)
    val c = rectangleCoordinates(2)
    val d = rectangleCoordinates(3)
    val pointCoordinates = pointString.split(',').map(_.toDouble)
    val x = pointCoordinates(0)
    val y = pointCoordinates(1)
    // check if the point is within the rectangle
    // consider on-boundary points
     x >= a && x <= c && y >= b && y <= d
  }

}
