package bilibili.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object TumblingWindowAll {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //spark,3
    //hadoop,2
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    val nums = lines.map(_.toInt)
    //不分组,将整体当成一个组
    val window  = nums.timeWindowAll(Time.seconds(5))

    val summed = window.sum(0)
    summed.print()

    env.execute("TumblingWindowAll")
  }
}
/**
 * 1,2,3
 * res:6
 * 没有数据不计算
 * 3
 * res:3
 */