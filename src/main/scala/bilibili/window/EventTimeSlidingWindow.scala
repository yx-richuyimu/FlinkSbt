package bilibili.window

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * 使用EventTime,划分滑动窗口
 * 如果使用的是并行的Source,例如KafkaSource,创建Kafka的Topic时有多个分区
 * 每一个Source的分区都满足触发的条件,整个窗口才会被触发
 */
object EventTimeSlidingWindow {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置EventTime作为时间标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1575017261000,spark,3
    //1575017262000,hadoop,2
    //仅仅提取时间字段,不会改变数据的样式
/*    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        //将数据中的时间字段提取出来,然后转成long类型
        override def extractTimestamp(line: String): Long = {
          val files = line.trim.split(",")
          files(0).toLong
        }
      })*/

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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[String](Time.seconds(0)) {
        //将数据中的时间字段提取出来,然后转成long类型
        override def extractTimestamp(line: String): Long = {
          val files = line.trim.split(",")
          files(0).toLong
        }
      })

    val wordAndCount = lines.map(x => (x.split(",")(1), x.split(",")(2).toInt))
    //先分组,再划分窗口
    val keyed = wordAndCount.keyBy(_._1)
    //划分窗口
    //划分窗口
    val window = keyed.window(SlidingEventTimeWindows.of(Time.seconds(6),Time.seconds(2)))

    window.sum(1).print()

    env.execute("EventTimeSlidingWindow")
  }
}
/**socket
 * 1000,a,1
 * 1999,a,1
 * res:(a,2)[0,1999](第一次2秒就触发,窗口长度要是滑动的倍数)(如果有水印,为1秒那就是在2999时触发[0,1999])
 * 2222,b,1
 * 2999,b,1
 * 4000,a,1
 * res:(a,3)
 *     (b,1)[0,3999]
 */
