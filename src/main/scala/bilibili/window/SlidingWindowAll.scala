package bilibili.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingWindowAll {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    val nums = lines.map(_.toInt)
    //不分组,将整体当成一个组
    //第一个5秒就会计算
    val window  = nums.timeWindowAll(Time.seconds(10), Time.seconds(5))

    val summed = window.sum(0)
    summed.print()

    env.execute("SlidingWindowAll")
  }
}
/**
 * 1,2,3
 * res:3
 *     6
 *     3
 *
 */