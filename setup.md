|Title |  Spark Installation |
|-----------|----------------------------------|
|Author | Kenneth Chen |
|Utility | Spark, Cassandra, Twitter |
|Date | 11/03/2018 |

# Architecture

<p align="center">
<img src="img/cassandra.png" width="800"></p>
<p align="center">Figure 1. Cassandra Architecture</p>

This is the simplification of Spark and Cassandra setup in our tweet analysis. Basically if you're retrieving tweets one time, you'd use `SparkContext()` and save the data in `rdd()`. However if you're streaming the tweets and would like to analyze on the fly, storing them in `rdd()` for quite sometime is risky because you don't know how long you want to stream and the capacity of the drive itself presents another issue in storage. To overcome this, you would like to stream tweets and feed them into database system where your tweets will be stored immediately and replicated across all available nodes you setup. In our example, we setup 3 nodes, and installed Spark in each of of them. Our master node is `spark1`. So the spark object is `StreamingContext`. 

### Note
You cannot use `SparkContext()` and `StreamingContext()` together in spark. You need to initiate one object. If you're initiating in `SparkContext()`, and would like to use for `StreamingContext()`, you need to 

```
val sc = new SparkContext(conf)
val ssc = new StreamingContext(sc, Seconds(1))
```
# Spark setup

Please go to <a href=https://github.com/kckenneth/Spark/blob/master/setup.md>setup</a> for detailed description. 

# Twitter setup

You'd need twitter API. Twitter has 4 essential keys. Prior to July 2018, by creating a twitter account, you can instantly get those APIs from `twitter application management`. Now, you need to apply a developer account and wait for the approval. 
```
consumerKey
consumerSecret
accessToken
accessTokenSecret
```

# Scala and Streaming Spark

Here you need to be familiar with scala program. Scala is a programming language written in java. Just like python program and its script in `.py`, scala script also has its suffix `.scala`. You also need to build all the dependencies for scala with `sbt` which some people call it `simple build tool` and others associate with `scala build tool`. You need two files: `build.sbt` and `xxx.scala`. 

We're going to analyze the tweets, so first create a directory 
```
$ mkdir /root/tweeteat
$ cd /root/tweeteat
```

## 1. build.sbt

In the `/root/tweeteat/` directory, you need to creat the `build.sbt` script. 
```
$ vi build.sbt
```
Copy and paste the following code. 
```
name := "Simple Project"
version := "1.0"
scalaVersion := "2.11.11"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
libraryDependencies += "org.apache.bahir" %% "spark-streaming-twitter" % "2.1.1"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.0.3"
resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
```
Spark version we installed earlier was `2.1.1`. 

## 2. tweeteat.scala

In the same directory `/root/tweeteat/` you need to create `tweeteat.scala` script. This script is the main scala script. Just create the script as in `$ vi tweeteat.scala`. Twitter 4 APIs are required. I removed the last 5 characters here for each of those for security. 

```
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter._

import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.SomeColumns

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

object TweatEat  extends App {
    val batchInterval_s = 1
    val totalRuntime_s = 32
    
    //add your creds below
    System.setProperty("twitter4j.oauth.consumerKey", "Mfkzw1lvN1TOoe2pCXiDxxxxx")
    System.setProperty("twitter4j.oauth.consumerSecret", "NNoVeO4zvozhIXDRvsvuLgr0XYPEcqX3ZGNbGfwYGE38Hxxxxx")
    System.setProperty("twitter4j.oauth.accessToken", "989972665686470657-tgMO3BBMrduV5dJt7sNWWp2xmTxxxxx")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "dDl8jlHDdeYm2GO60MZpZjLlzdNPFWTVBk05zUgmxxxxx")
    
    // create SparkConf
    val conf = new SparkConf()
        .setAppName("mids tweeteat")
        .set("spark.cassandra.connection.host", "127.0.0.1")
        .set("spark.cassandra.connection.connections_per_executor_max", "2");

    // batch interval determines how often Spark creates an RDD out of incoming data
    val ssc = new StreamingContext(conf, Seconds(30))
    val stream = TwitterUtils.createStream(ssc, None)

    // extract desired data from each status during sample period as class "TweetData", store collection of those in new RDD
    //val tweetData = stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim)).saveToCassandra("streaming", "tweetdata", SomeColumns("id", "author", "tweet"))
    stream.map(status => TweetData(status.getId, status.getUser.getScreenName, status.getText.trim)).saveToCassandra("streaming", "tweetdata", SomeColumns("id", "author", "tweet"))
     //tweetData.foreachRDD(rdd => {
    // data aggregated in the driver
     //println(s"A sample of tweets I gathered over ${batchInterval_s}s: ${rdd.take(10).mkString(" ")} (total tweets fetched: ${rdd.count()})")})
    
    // start consuming stream
    ssc.start
    ssc.awaitTerminationOrTimeout(totalRuntime_s * 1000)
    ssc.stop(true, true)

    println(s"============ Exiting ================")
    System.exit(0)
}

case class TweetData(id: Long, author: String, tweet: String)
```
--------------
# Cassandra setup

Since you're going to stream tweets, you need to setup Cassandra which will process your tweet data on the fly. Basically, every tweet will be stored in Cassandra where we can create a table (noSQL), customize the columns and such. So every tweets information will be stored in Cassandra noSQL table. When we're done with the streaming, we can launch Cassandra noSQL and process our tweet data. We will setup the Cassandra as follow. 

Create a script `datastax.repo` in the following directory. 

```
vi /etc/yum.repos.d/datastax.repo`
```
Copy and paste the code below
```
[datastax]
name = DataStax Repo for Apache Cassandra
baseurl = http://rpm.datastax.com/community
enabled = 1
gpgcheck = 0
```

## Install Cassandra database
You'd probably remember, we are now in CentOS, so our intallation command is `yum`, not `apt-get` in Ubuntu. 
```
$ yum -y install dsc20
```

## Start Cassandra
Use either of the commands to start cassandra. 
```
$ systemctl start cassandra
```
Or
```
/etc/init.d/cassandra start
```
## Check Cassandra
Check if the cassandra is already started. 
```
$ cqlsh

Connected to Test Cluster at localhost:9160.
[cqlsh 4.1.1 | Cassandra 2.0.17 | CQL spec 3.1.1 | Thrift protocol 19.39.0]
Use HELP for help.
```
If so, quit by `ctrl+d` or `exit`



# 1. Compile the scala app
Now you have all the script, you first compile the script. This will generate `project` and `target` directories in your current folder. So everytime you make changes in the scala script, you need to recompile it again. 
```
$ sbt clean package
```

#### Note
If you want to compile and run the program immediately, you can also do so by `$ sbt package run`. However this will create an error because you'd need to provide the spark node information. So we will compile and run the program separately. 

Now that two directories are created: `project` and `target`, the java file we need when we stream the tweets into our spark is in the `target` directory. You can double check by 
```
$ find . -iname "*.jar"
```
# 2. Stream the tweets !

You need 3 flags when you `spark-submit`.   
--master  
--packages  
--class

`--class` flag, you specificy the class object from your `tweeteat.scala` script. It's from `object TweatEat  extends App {`. 

```
$SPARK_HOME/bin/spark-submit --master spark://spark1:7077  --packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  --class TweatEat $(find target -iname "*.jar") 
```









```
cqlsh> select * from streaming.tweetdata;

 id                  | author          | tweet
---------------------+-----------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 1058815759898632192 |   britneyroslyn |                                                                                                                                                                                               RT @AmoNickk: a 4 for $4 at chick-fil-a https://t.co/AkvJSC14ao
 1058815738914504707 |     HabbalFares |                                                                                                                                                                                                                              @AskJayne #Northstrong #Burnblue
 1058815680194199552 | CrazyCatMan1979 |                                                                                                                                                          @KIERAHLAGRAVE Why, so you can kill innocent guys instead of simply choking them till they pass out?
 1058815734715965440 |     abby2cullen |                                                                                                                  RT @FoxsGlaciers: #FlashbackFriday and we’ve selection boxes to #giveaway as a #FreebieFriday To #enter the #prizedraw and win a #Retro #Pr…
 1058815730517467137 |       new__kidz |                                                                                                                                                                                     non sapevo servisse un rito di iniziazione per definirsi fan di un gruppo
 1058815730530111492 |    loonarmyxoxo |                                                                                                                                                                                                                       RT @sapphofiIm: https://t.co/bZFyUuHav6
 1058815659218415616 |      jt_powell7 |                                                                                                                                                                                                   RT @seven_thenumber: so,,, is everyone like,,, goin thru it
 1058815675991576586 |    FernandoPL21 |                                                                                                                                                                                                                                       Las mejores amistades😌🎃
```







