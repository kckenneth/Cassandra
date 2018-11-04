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
log4j.rootCategory=WARN, console
```
# Twitter Popularity
We will create a new directory where we stream tweet and process. We will create two main files: `build.sbt` and `twitter_popularity.scala`. 

The directory will look like this. 
```
/root/twitter_popularity
├── build.sbt
└── twitter_popularity.scala
```
## build.sbt
```
$ mkdir /root/twitter_popularity
$ cd /root/twitter_popularity
$ vi build.sbt
```
Copy and paste the following code
```
name := "Simple Project"
version := "1.0"
scalaVersion := "2.11.11"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
libraryDependencies += "com.typesafe" % "config" % "1.3.0"
resolvers += "Akka Repository" at "http://repo.akka.io/releases"
```
Our spark version is `2.1.1`. I also added cassandra and typeface to be on the safe side. 

## twitter_popularity.scala
```
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.SparkConf
import twitter4j._
import org.apache.spark.streaming.twitter._

import org.apache.spark._
import org.apache.spark.streaming._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config

import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar



object Main extends App {
    println(s"\nI got executed with ${args size} args, they are: ${args mkString ", "}\n")

  // your code goes here
  // get user input for batch duration, window duration, top n hashtags, and filters
    val batchDur = args(0).toInt  //batch duration
    val winDur = args(1).toInt    //aggregation window duration
    val topN = args(2).toInt
    val filters = args.takeRight(args.length - 3)

    val format = new SimpleDateFormat("HH:mm:ss")

    println("Input: {batch : %d, win : %d, topN:%d}".format(batchDur, winDur,topN))

    // Twitter4j library was configured to generate OAuth credentials
    //val propPrefix = "twitter4j.oauth."
    //System.setProperty(s"${propPrefix}consumerKey", "ccwtl2crmKH0p30pHocmcvouh")
    //System.setProperty(s"${propPrefix}consumerSecret", "gMjli4yVo2Bf3kM3ejHez1vDtB5AqK7r2HTqk9pvx9GNEF7F9n")
    //System.setProperty(s"${propPrefix}accessToken", "1414214346-SIvjGchjh09A6r3YIZzTNSaj0LOBF3kqJL6syrW")
    //System.setProperty(s"${propPrefix}accessTokenSecret", "2EHxskNPxczHxsjmrqiA1C1peydSjNxf9kHiwPqwSUqZ0")
    
    System.setProperty("twitter4j.oauth.consumerKey", "Mfkzw1lvN1TOoe2pCXiDFcQSP")
    System.setProperty("twitter4j.oauth.consumerSecret", "NNoVeO4zvozhIXDRvsvuLgr0XYPEcqX3ZGNbGfwYGE38HhQEud")
    System.setProperty("twitter4j.oauth.accessToken", "989972665686470657-tgMO3BBMrduV5dJt7sNWWp2xmTRqitC")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "dDl8jlHDdeYm2GO60MZpZjLlzdNPFWTVBk05zUgmb6otR")
    
    val sparkConf = new SparkConf()
        .setAppName("mids tweeteat")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.cassandra.connection.connections_per_executor_max", "2");
        
    //val sparkConf = new SparkConf().setAppName("TwitterPopularTags")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDur))
    val stream = TwitterUtils.createStream(ssc, None, filters)
    println(s"This is stream {%s}".format(stream)) 

    // extract desired data from each status during sample period as class "TweetData", store collection of those in new RDD
    val tweetData = stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim)).saveToCassandra("streaming", "tweetdata", SomeColumns("id", "author", "tweet"))
    //tweetData.foreachRDD(rdd => {println(s"A sample of tweets I gathered over ${batchDur}s: ${rdd.take(10).mkString(" ")} (total tweets fetched: ${rdd.count()})")})
    
    val hashKeyVal = stream.map(tweet => {
        val hashtags = tweet.getText().split(" ").filter(word => word.startsWith("#"))
        val authors  = tweet.getUser.getScreenName()
        val mentions = tweet.getUserMentionEntities().map(_.getScreenName).toArray
        (hashtags, Set(authors), mentions)})
        .flatMap(tup => tup._1.map(hashtag => (hashtag, (1, tup._2, tup._3))))
        
    // Reduce by counting hashtag occurances and creating a set of users and mentions
    // Then sort by count
    val hashSortedCnt = hashKeyVal
                     .reduceByKeyAndWindow({case (x, y) =>
                                   (x._1 + y._1, x._2 ++ y._2, x._3 ++ y._3)}, Seconds(winDur))
                     .transform(_.sortBy({case (_, (count, _, _)) => count}, ascending = false))
                     
    // Take top N hashtags and print results       
    hashSortedCnt.foreachRDD( rdd => {
        var topHashes = rdd.take(topN)
        println("\n")
        println(format.format(Calendar.getInstance().getTime()))
        println("Top %d hashtags in last %d seconds (%s total):".format(topN, winDur, rdd.count()))
        topHashes.foreach{ case (tag, (cnt,users,mentions)) =>
                      println("  %s (%s tweets)".format(tag, cnt))
                      println("     Users (%s):    %s".format(users.size, users.mkString(",")))
                      println("     Mentions (%s): %s".format(mentions.size, mentions.mkString(",")))
                      }
        })
        
    // start consuming stream
    ssc.start
    ssc.awaitTerminationOrTimeout(winDur * 1000)
    ssc.stop(true, true)

    println(s"============ Exiting ================")
    System.exit(0)
}

case class TweetData(id: Long, author: String, tweet: String)



```


# Build the app
```
$ sbt package
```
The directory will look like this after compiling the scala. 
```
/root/twitter_popularity
├── build.sbt
├── project
├── target
└── twitter_popularity.scala
```
