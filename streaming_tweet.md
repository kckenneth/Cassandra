# Streaming Tweet and Processing

In this exercise, we're going to stream Tweeet with parameters such as 
1. total duration (TotD), 
2. Interval (Intv), 
3. Top most occurring hashtags (TopHT), and 
4. filters words (Flts) 

# Making less verbose in spark-submit
Since spark-submit generates lots of INFO in console which we find it distracting, we will modify the log file in spark. 
To make spark-submit less verbose, 
```
$ cd /usr/local/spark/conf
```

Spark has a default template `log4j.properties.template`. 
```
$ cp log4j.properties.template log4j.properties
$ vi log4j.properties
```
You'd copy the template because `log4j.properties` is going to be a main log file. You need to change `log4j.rootCategory=INFO, console` into `log4j.rootCategory=WARN, console`. 
```
log4j.rootCategory=INFO, console
```

```
log4j.rootCategory=WARN, console
```





```
./twitter_popularity
├── build.sbt
├── project
│   └── plugins.sbt
└── twitter_popularity.scala
```
