# Execution steps

```
$ ssh root@50.97.252.101
```

Enter the password. 

Spart-submit with 
- 10s interval  
- 30s total duration  
- 5 top hashtags 

```
$SPARK_HOME/bin/spark-submit --master spark://spark1:7077  --packages org.apache.bahir:spark-streaming-twitter_2.11:2.1.0,com.datastax.spark:spark-cassandra-connector_2.11:2.0.3  --class Main $(find target -iname "*.jar") 10 30 5
```

