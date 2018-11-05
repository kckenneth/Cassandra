# Architecture

<p align="center">
<img src="img/cassandra.png" width="600"></p>
<p align="center">Figure 1. Cassandra Architecture</p>

# Execution steps

```
$ ssh root@50.97.252.101
# cd /root/twitter_popularity
```

Enter the password. 

spark-submit with 
- 10s interval  
- 60s total duration  
- 5 top hashtags 

```
$SPARK_HOME/bin/spark-submit --master spark://spark1:7077  --packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  --class Main $(find target -iname "*.jar") 10 60 5
```

# Sample output
```
18:21:00
Top 5 hashtags in last 60 seconds (256 total):
  #AprendiNoEnem (8 tweets)
     Users (8):    lafodirection,GeoDuarte2,_RubiaGabriella,fwayhaughtvr,Mateusevx,mica_s2_,santos_viriato,cl4cl44
     Mentions (8): tisgopjl,AprendiNo_ENEM,AprendiNo_ENEM,greysanatombei,AprendiNo_ENEM,AprendiNo_ENEM,limigabriel,AprendiNo_ENEM
  #euvoudeproenem (6 tweets)
     Users (1):    AnaLetciaArajo3
     Mentions (0): 
  #MTVEMA (4 tweets)
     Users (4):    fwsps1fq,yoongiswings,mikaelapenzotti,DanindraAyu
     Mentions (7): MTV,BTS_twt,btsanalytics,BTS_twt,JackAndJackAR,mixermanagement,LittleMix
  #Texans (3 tweets)
     Users (3):    therealhectorg,NBoone_99,iam_MR713
     Mentions (4): GregBailey13,JJWatt,TexansPR,HoustonTexans
  #Mickey90 (3 tweets)
     Users (3):    ybblld,himalaiii,aishaishbby
     Mentions (3): ABCNetwork,NCTsmtown_127,NCTsmtown_127
```
---------------
# Details

1. <a href=https://github.com/kckenneth/Cassandra/blob/master/setup.md>How to setup spark, cassandra</a>
2. <a href=https://github.com/kckenneth/Cassandra/blob/master/streaming_tweet.md>Streaming Tweet</a> 
3. <a href=https://github.com/kckenneth/Cassandra/blob/master/execution.md>Execution</a>

# Interesting Facts

Verbosity of log in spark-submit can be removed in conf directory. Since scala is a functional programming language, each input can be processed by `map`, `flatmap` to cater to our needs. There are many things I learned during the exercise. One of them is table in cassandra becomes too big after I run `spark-submit` several times. So when I called `cqlsh` next time, `rpc timeout` pops up. I detailed how I troubleshoot in my <a href=https://github.com/kckenneth/Cassandra/blob/master/streaming_tweet.md> streaming tweet and process</a> post. 

