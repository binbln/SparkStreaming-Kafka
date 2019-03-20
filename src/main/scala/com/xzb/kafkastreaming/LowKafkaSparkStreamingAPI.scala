package com.xzb.kafkastreaming

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.immutable.HashMap

/**
  * xzb
  */
object LowKafkaSparkStreamingAPI {

  def getOffsets(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {

    //定义一个最终的返回值 主题分区->offset
    var partitionToLong = new HashMap[TopicAndPartition, Long]()

    //根据指定的topic获取分区
    val topicAndPartitionEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))

    //判断分区是否存在
    if (topicAndPartitionEither.isRight) {
      //分区信息不为空

      // 取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicAndPartitionEither.right.get

      //获取消费者消费数据的进度 offset

      val topicAndPartitionToLongEither: Either[Err, Map[TopicAndPartition, Long]] = kafkaCluster.getConsumerOffsets(group, topicAndPartitions)

      //判断offset 是否存在
      if (topicAndPartitionToLongEither.isLeft) {

        //offset 不存在(该消费者组未消费过),遍历每一个分区
        for (topicAndPartition <- topicAndPartitions) {
          partitionToLong += (topicAndPartition -> 0L)
        }

      } else {

        //offset 存在 取出
        val topicAndPartitions: Map[TopicAndPartition, Long] = topicAndPartitionToLongEither.right.get

        partitionToLong ++= topicAndPartitions
      }

    }

    partitionToLong
  }

  def setOffset(kafkaCluster: KafkaCluster, group: String, kafkaDataStream: InputDStream[String]) = {

    kafkaDataStream.foreachRDD(rdd => {

      //取出rdd中的offset
      val hasOffsetRanges: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]

      //获取所有分区的offsetRange
      val offsetRanges: Array[OffsetRange] = hasOffsetRanges.offsetRanges

      //遍历offsetRanges
      for (range <- offsetRanges) {

        kafkaCluster.setConsumerOffsets(group, HashMap(range.topicAndPartition() -> range.untilOffset))

      }
    })

  }

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("LowKafkaSparkStreaming")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //kafka配置
    val brokers = "hadoop107:9092,hadoop108:9092,hadoop109:9092"
    val zk = "hadoop107:2181,hadoop108:2181,hadoop109:2181"
    val topic = "test"
    val group = "lowConsumer"
    val deserializationClass = "org.apache.kafka.common.serialization.StringSerializer"


    val kafkaParams = Map(
      "zookeeper.connect" -> zk,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserializationClass,
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "smallest"
    )

    val kafkaCluster = new KafkaCluster(kafkaParams)

    val fromOffsets: Map[TopicAndPartition, Long] = getOffsets(kafkaCluster, group, topic)

    //消费kafka数据创建DStream
    val kafkaDataStream: InputDStream[String] =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
        ssc,
        kafkaParams,
        fromOffsets,
        (msg: MessageAndMetadata[String, String]) => msg.message()
      )

    //打印
    kafkaDataStream.print()

    //保存offset
    setOffset(kafkaCluster, group, kafkaDataStream)

    //启动
    ssc.start()
    ssc.awaitTermination()
  }

}
