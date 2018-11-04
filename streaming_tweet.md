# Streaming Tweet and Processing

In this exercise, we're going to stream Tweeet with parameters such as 
1. total duration (TotD), 
2. Interval (Intv), 
3. Top most occurring hashtags (TpHT), and 
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
### build.sbt
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

### twitter_popularity.scala
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
    // get user input for total duration, interval, number of top hashtags, and filter words (optional)
    val TotD = args(0).toInt  
    val Intv = args(1).toInt    
    val TpHT = args(2).toInt
    val Flts = args.takeRight(args.length - 3)

    val format = new SimpleDateFormat("HH:mm:ss")
    
    System.setProperty("twitter4j.oauth.consumerKey", "Mfkzw1lvN1TOoe2pCXiDxxxxx")
    System.setProperty("twitter4j.oauth.consumerSecret", "NNoVeO4zvozhIXDRvsvuLgr0XYPEcqX3ZGNbGfwYGE38Hxxxxx")
    System.setProperty("twitter4j.oauth.accessToken", "989972665686470657-tgMO3BBMrduV5dJt7sNWWp2xmTxxxxx")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "dDl8jlHDdeYm2GO60MZpZjLlzdNPFWTVBk05zUgmxxxxx")
    
    val sparkConf = new SparkConf()
        .setAppName("mids tweeteat")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.cassandra.connection.connections_per_executor_max", "2");
        
    val ssc = new StreamingContext(sparkConf, Seconds(TotD))
    val stream = TwitterUtils.createStream(ssc, None, Flts)

    // extract desired data from each status during sample period as class "TweetData", store collection of those in new RDD
    stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim)).saveToCassandra("streaming", "tweetdata", SomeColumns("id", "author", "tweet"))
 
    // Split the tweet into 3 separate informations: hashtags, user and mentions. 
    val hashKeyVal = stream.map(tweet => {
        val hashtags = tweet.getText().split(" ").filter(word => word.startsWith("#"))
        val authors  = tweet.getUser.getScreenName()
        val mentions = tweet.getUserMentionEntities().map(_.getScreenName).toArray
        (hashtags, Set(authors), mentions)})
        .flatMap(tup => tup._1.map(hashtag => (hashtag, (1, tup._2, tup._3))))
        
    // Reduce by counting the hashtags and sort by the count 
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

# Stream the Tweet !
We will stream the tweet for 30 seconds with 10 second intervals process. Top 3 hashtags will be listed. 
```
$SPARK_HOME/bin/spark-submit --master spark://spark1:7077  --packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  --class Main $(find target -iname "*.jar") 10 30 3 
```
Output  
Since we are processing every 10 seconds for 30 seconds in total, there will be 3 outputs. Each output has top 3 hashtags. 
```
14:29:51
Top 3 hashtags in last 10 seconds (22 total):
  #MTVEMA (2 tweets)
     Users (2):    Anxiety__ridden,toxicskylie
     Mentions (1): panicupdating
  #MAGA (1 tweets)
     Users (1):    yeojinakgae
     Mentions (1): likeysIut
  #السعودية_كوريا_الجنوبية
 (1 tweets)
     Users (1):    BandarKhalid_10
     Mentions (1): hamadfahadhh

14:30:05
Top 3 hashtags in last 10 seconds (49 total):
  #MTVEMA (2 tweets)
     Users (2):    Anxiety__ridden,toxicskylie
     Mentions (1): panicupdating
  #MAGA (1 tweets)
     Users (1):    yeojinakgae
     Mentions (1): likeysIut
  #HauntingOfHillHouse (1 tweets)
     Users (1):    SuperSuperNico
     Mentions (1): nexuspong
 
14:30:06
Top 3 hashtags in last 10 seconds (27 total):
  #ElectionDay (1 tweets)
     Users (1):    johnnywandermer
     Mentions (1): Lola15363615
  #RealSociedadSevillaFC (1 tweets)
     Users (1):    RealSociedadEUS
     Mentions (0): 
  #HotTicketCandidates (1 tweets)
     Users (1):    deanbc1
     Mentions (1): WarriorofGod97
```

     

