package bilibili.source.realtime

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * 从kafka中读取数据的SOURCE,是可以并行的SOURCE,并且可以实现ExactlyOnce
 */
object KafkaSource {
  def main(args: Array[String]): Unit = {
    val env =StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    //指定kafka的Broker地址
    properties.setProperty("bootstrap.servers", "yangxu-virtual-machine:9092")
    //指定组ID
    properties.setProperty("group.id","test")
    //如果没有记录偏移量,第一次从最开始消费
    properties.setProperty("auto.offset.reset","earliest")
    //kafka的消费者不自动提交偏移量
    //properties.setProperty("enable.auto.commit", "false")

    //kafkaSource
    val kafkaSource = new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties)

    //source
    val lines = env
      .addSource(kafkaSource)

    //sink
    lines.print()

    env.execute("KafkaSource")
  }
}
