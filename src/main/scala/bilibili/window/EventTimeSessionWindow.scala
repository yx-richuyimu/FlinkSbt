package bilibili.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time

object EventTimeSessionWindow {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //设置EventTime作为时间标准
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1575017261000,spark,3
    //1575017262000,hadoop,2
    //仅仅提取时间字段,不会改变数据的样式
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)
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
    val window = keyed.window(EventTimeSessionWindows.withGap(Time.seconds(5)))

    window.sum(1).print()

    env.execute("EventTimeSessionWindow")
  }
}
/**
 * 1575017261000,spark,3
 * 1575017262000,spark,3(1575017262000与上一个相差1秒)
 * 1575017267000,spark,1(5秒)
 * 1575017262100,spark,1(5.1秒)
 * 结果:(spark,7)
 *
 */