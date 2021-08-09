package bilibili.util

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

object FlinkUtilsV1 {
  private val env =StreamExecutionEnvironment.getExecutionEnvironment

  def createKafkaStream(args: Array[String], simpleStringSchema :SimpleStringSchema): DataStream[String] = {
    val topic = args(0)
    val groupId = args(1)
    val brokerList = args(2)

    val properties = new Properties()
    //指定kafka的Broker地址
    properties.setProperty("bootstrap.servers", brokerList)
    //指定组ID
    properties.setProperty("group.id",groupId)
    //如果没有记录偏移量,第一次从最开始消费
    properties.setProperty("auto.offset.reset","earliest")
    //kafka的消费者不自动提交偏移量
    //properties.setProperty("enable.auto.commit", "false")

    //kafkaSource
    val kafkaSource = new FlinkKafkaConsumer[String](topic, new SimpleStringSchema(), properties)

    //source
    env.addSource(kafkaSource)
  }

  def getEnv(): StreamExecutionEnvironment = {
    env
  }
}
