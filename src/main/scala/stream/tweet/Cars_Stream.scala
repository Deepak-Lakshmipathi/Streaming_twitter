package stream.tweet

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import twitter4j._
import twitter4j.auth.Authorization

/*
class auth1(val cK: String,val cSK: String, val aT: String, val aST: String) {
  val auth = new Authorization {
    override def getAuthorizationHeader(req: HttpRequest): String = ???

    override def isEnabled: Boolean = ???
  }
  val config = new twitter4j.conf.ConfigurationBuilder()
    .setOAuthConsumerKey(cK)
    .setOAuthConsumerSecret(cSK)
    .setOAuthAccessToken(aT)
    .setOAuthAccessTokenSecret(aST)
    .build
  val twitter_auth = new TwitterFactory(config)
  val a = new twitter4j.auth.OAuthAuthorization(config)
  val atwitter =  twitter_auth.getInstance(a).getAuthorization()
}
*/

object Cars_Stream {
  def main(args: Array[String]): Unit = {

    if (args.length < 4){
      println("need more args")
      System.exit(1)
    }

    val Array(cK,cS,aT,aTS) = args.take(4)

    System.setProperty("twitter4j.oauth.consumerKey",cK)
    System.setProperty("twitter4j.oauth.consumerSecret",cS)
    System.setProperty("twitter4j.oauth.accessToken",aT)
    System.setProperty("twitter4j.oauth.accessTokenSecret",aTS)


    //val auth2 = new auth1(args(0),args(1),args(2),args(3))
    val filter: Array[String] = Array("Cars")
    val Session1 = SparkSession.builder().appName("Streaming_test").getOrCreate()
    //val conf = new SparkConf().setAppName("Streaming_test")
    val ssc = new StreamingContext(Session1.sparkContext,Seconds(10))

    val st = TwitterUtils.createStream(ssc,None,filter)

    val hashTags = st.flatMap(x => x.getText.split(" ").filter(_.startsWith("#")))
    val top_10 = hashTags.map((_,1)).reduceByKeyAndWindow(_+_, Seconds(10))
      .map{case (topic,count)=> (count,topic)}.transform(_.sortByKey(false))

    top_10.foreachRDD(rdd => {
      val top_List = rdd.take(10)
      println("number of topics (%s total):".format(rdd.count()))
      top_List.foreach{case (count,tag)=> println("Hashtag :%s(%s tweets)".format(tag,count))}
    })


    ssc.start()
    ssc.awaitTermination()
  }

}
