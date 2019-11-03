package stream.tweet

import org.apache.spark.streaming.{Seconds,StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import twitter4j._

object auth{
  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey("JNWGw9aBnVMYO2O29KDeg1AOH")
    .setOAuthConsumerSecret("8Dtm62H6Ditso7I7wOb7dAppQNlOc25jDA3yy0flxPIl0I33on")
    .setOAuthAccessToken("109191571-lqLrCsRDicYJxqx5fDEWcbHE8TUmPGzW7BPXdEok")
    .setOAuthAccessTokenSecret("2qGJVYSoYR2sZ5WSCfmZhTD6KSRP0iSfrKHP464l4vQvm")
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

    val test = st.flatMap(x => x.getText.split(" "))

  }

}
