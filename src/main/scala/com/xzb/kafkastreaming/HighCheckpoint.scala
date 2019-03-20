package com.xzb.kafkastreaming

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  * xzb
  */
object HighCheckpoint {

  def createSSC(): StreamingContext = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafkaSparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("./checkpoint/test/highConsumer")
    val brokers = "hadoop107:9092,hadoop108:9092,hadoop109:9092"
    val zk = "hadoop107:2181,hadoop108:2181,hadoop109:2181"
    val topic = "test"
    val group = "highConsumer"
    val deserializationClass = "org.apache.kafka.common.serialization.StringSerializer"


    val kafkaParams = Map(
//      "zookeeper.connect" -> zk,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializationClass
    )

    //消费kafka数据 创建DStream
    val kafkaDataStream: InputDStream[(String, String)]
    = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topic))

    //打印
    kafkaDataStream.print

    ssc
  }

  def main(args: Array[String]): Unit = {

    //获取ssc
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("./checkpoint/test/highConsumer", () => createSSC())


    ssc.start()
    ssc.awaitTermination()
  }
}
