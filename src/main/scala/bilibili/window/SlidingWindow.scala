package bilibili.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object SlidingWindow {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //spark,3
    //hadoop,2
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    val wordAndCount = lines.map(x => (x.split(",")(0), x.split(",")(1).toInt))
    //先分组,再划分窗口
    val keyed = wordAndCount.keyBy(_._1)
    //划分窗口
    val window = keyed.timeWindow(Time.seconds(10), Time.seconds(5))
    //keyed.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
    window.sum(1).print()

    env.execute("SlidingWindow")
  }
}
/**a,1
 * a,1
 * res:(a,2)两个在一个时段里
 *     (a,2)
 * c,1
 * c,1
 * res:(c,1)
 *     (c,2)
 *     (c,1)
 * a,1
 * a,1
 * c,1
 * c,1
 * res:(a,2)
 *     (c,2)
 *     (a,2)
 *     (c,2)
 */