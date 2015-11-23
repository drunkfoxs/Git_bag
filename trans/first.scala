val data = "datamine/input/hubei_cdr"
val gsm = "datamine/input/maps/gsm.csv"
val sdata = sc.textFile(gsm)

val da = sc.textFile(data).map( x => x.split("\001")).filter( x => x(0)!="2")
val user_group = da.map( x => (x(0),x(1)+"|"+x(2)+"|"+x(3))).reduceByKey(_+","+_)
val user_sort = user_group.map( x => {
    val xlist = x._2.split(",",-1).toList
    val xsort = xlist.sortBy{ case(y) => y.split("\\|")(0).replaceAll("[- :]","")}
    (x._1,xsort)
}).filter(_._2.size>1)

def calc_distance(start_lat: String, start_lng: String, end_lat: String, end_lng: String): Long = {
  val AVERAGE_RADIUS_OF_EARTH = 6371
  val userLat = start_lat.toDouble
  val userLng = start_lng.toDouble
  val venueLat = end_lat.toDouble
  val venueLng = end_lng.toDouble

  val latDistance = Math.toRadians(userLat - venueLat)
  val lngDistance = Math.toRadians(userLng - venueLng)

  val a = Math.sin(latDistance / 2) * Math.sin(latDistance / 2) +
                   Math.cos(Math.toRadians(userLat)) * Math.cos(Math.toRadians(venueLat)) *
                   Math.sin(lngDistance / 2) * Math.sin(lngDistance / 2)

                   val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

                   return Math.round(AVERAGE_RADIUS_OF_EARTH * c)
}

val mapdata = sdata.filter(x => x.split(",").length > 18).map(x=> (x.split(",")(1)+"_"+x.split(",")(2),x.split(",")(16)+"|"+x.split(",")(17))).collectAsMap
val data_2 = user_sort.map{x =>
     var xx = x._2
     var yy = List[String]()
     for (i <- (0 until xx.size - 1)) {
        var temp = xx(i).split("\\|")
        var location = mapdata.getOrElse(temp(2),"error").split("\\|")
        yy = yy :+ (temp(0) + "|" + temp(1) + "|" + location(0) + "|" + location(1))
     }
     (x._1,yy)
}



