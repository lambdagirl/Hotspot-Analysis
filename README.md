# New York Taxi Hotspot Analysis
#### Introduction
New York City is famous for its yellow taxis. When to take the taxi and where is the pickup location will be useful for both taxi drivers and travelers in the five boroughs. The New York Trip dataset input is from January 2009 to June 2015, contains both temporal data and geospatial information. By using Apache Spark, Scala and Getis-Ord statistic technic, data scientist can easily identify the statistically significant spatial hot spot, which can be used as recommendation for tax drivers the best pick up location, as well the recommendations for passengers the best time and the best place to travel with a taxi, and companies like Google Maps. The primary objective is to identify the fifty most statically significant drop-off locations by passenger count in both time and space using the Getis-Ord statistic. Space will be aggregated into a grid (using latitude and longitude); time will be aggregated into time windows (one-hour periods).


![alt text](http://sigspatial2016.sigspatial.org/giscup2016/gfx/image013.jpg "title")


