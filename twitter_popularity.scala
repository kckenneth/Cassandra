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
 
    // Split the tweet into 3 separate informations: hashtags, user and mentions, create in a tuple, and flatmap
    val hash_usr_men = stream.map(tweet => {
        val hashtags = tweet.getText().split(" ").filter(word => word.startsWith("#"))
        val authors  = tweet.getUser.getScreenName()
        val mentions = tweet.getUserMentionEntities().map(_.getScreenName).toArray
        (hashtags, Set(authors), mentions)})
        .flatMap(tupl => tupl._1.map(hashtag => (hashtag, (1, tupl._2, tupl._3))))
        
    // Reduce by counting the hashtags and sort by the count 
    val hash_sorted = hash_usr_men
                     .reduceByKeyAndWindow({case (x, y) => (x._1 + y._1, x._2 ++ y._2, x._3 ++ y._3)}, Seconds(Intv))
                     .transform(_.sortBy({case (_, (count, _, _)) => count}, ascending = false))
                     
    // Take top N hashtags and print results       
    hash_sorted.foreachRDD( rdd => {
        var topHashes = rdd.take(TpHT)
        println("\n")
        println(format.format(Calendar.getInstance().getTime()))
        println("Top %d hashtags in last %d seconds (%s total):".format(TpHT, Intv, rdd.count()))
        topHashes.foreach{ case (tag, (cnt,users,mentions)) =>
                      println("  %s (%s tweets)".format(tag, cnt))
                      println("     Users (%s):    %s".format(users.size, users.mkString(",")))
                      println("     Mentions (%s): %s".format(mentions.size, mentions.mkString(",")))
                      }
        })
        
    // start consuming stream
    ssc.start
    ssc.awaitTerminationOrTimeout(Intv * 1000)
    ssc.stop(true, true)

    println(s"============ Exiting ================")
    System.exit(0)
}

case class TweetData(id: Long, author: String, tweet: String)
