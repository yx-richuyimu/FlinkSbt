package bilibili.window

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import java.util.Properties

/**
 * 使用EventTime,划分滚动窗口
 * 如果使用的是并行的Source,例如KafkaSource,创建Kafka的Topic时有多个分区
 * 每一个Source的分区都满足触发的条件,整个窗口才会被触发
 */
object EventTimeTumblingWindow {
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
    val window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)))

    window.sum(1).print()

    env.execute("EventTimeSessionWindow")
  }
}
/**socket
 * 1000,a,1
 * 2000,a,1
 * 4998,a,1
 * 4999,a,1
 * res:(a,4) 滑动窗口会自动对齐,1000从[0000,4999],[5000,9999]前提:窗口长度5秒,Flink划分窗口的时间精确到毫秒.窗口是前闭后开的
 * 6999,a,1
 * 8888,a,1
 * 9998,a,1
 * 12000,a,1
 * res:(a,3)
 * 14999,a,1
 * res:(a,2)
 * 15000,a,1
 * 15001,b,1
 * 1998,a,1
 * 1999,a,1
 * res:(a,3)
 *     (b,1)(socket并行度为1,只有一个分组,所以不会打印)
 */
/**socket 计算过的窗口,迟到的数据不做计算
 * 1000,a,1
 * 2000,a,1
 * 3000,b,1
 * 4999,c,1
 * res:(c,1)
 *     (b,1)
 *     (a,2)
 * 4000,a,1
 * 4444,a,1
 * 8888,a,1
 * 9999,b,1
 * res:(a,1)
 *     (b,1)
 */
/**socket assignTimestampsAndWatermarks为2秒
 * WaterMark = 数据所携带的时间(窗口中的最大时间) - 延迟执行的时间
 * WaterMark >= 上一个窗口的结束边界就会出发窗口执行(并行每一个分区都要满足)
 * WaterMark是Flink中窗口延迟出发的机制
 * 1000,a,1
 * 4998,a,1
 * 4999,a,1
 * 5000,a,1
 * 6999,b,1
 * res:(a,3)6999触发第一个窗口的时间
 * 8888,c,1
 * 9999,d,1
 * 11111,a,1
 * 15000,a,1
 * res:(a,1)
 *     (b,1)
 *     (c,1)
 *     (d,1)
 */
/** kafkaSource 分区为3
 * 1000,a,1
 * 4999,a,1
 * 4999,a,1
 * 4999,a,1
 * res:(a,4)
 * 5001,a,1
 * 5002,b,1
 * 9999,a,1
 * 9999,b,1
 * 9999,c,1
 * res:(b,2)
 *     (c,1)
 *     (a,2)(所有分区都满足条件时才会被触发,可能会出现堆积)
 *
 */