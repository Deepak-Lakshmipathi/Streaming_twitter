package stream.tweet

import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import twitter4j._

object auth{
  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("")
    .setOAuthConsumerSecret("")
    .setOAuthAccessToken("")
    .setOAuthAccessTokenSecret("")
    .build
  val twitter_auth = new TwitterFactory(auth.config)
  val a = new twitter4j.auth.OAuthAuthorization(auth.config)
  val atwitter =  twitter_auth.getInstance(a).getAuthorization()
}


object Cars_Stream {
  def main(args: Array[String]): Unit = {
    val filter: Array[String] = Array("Cars")
    val Session1 = SparkSession.builder().appName("Streaming_test").getOrCreate()
    val conf = new SparkConf().setAppName("Streaming_test")
    val ssc = new StreamingContext(conf,Seconds(10))

    val st = TwitterUtils.createStream(ssc,Some(auth.atwitter),filter)

    val hashTags = st.flatMap(x => x.getText.split(" ").filter(_.startsWith("#")))
    val top_10 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(60))
      .map{case (topic,count)=> (count,topic)}.transform(_.sortByKey(false))
  }

}
