package bilibili.window

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows
import org.apache.flink.streaming.api.windowing.time.Time


object SessionWindow {
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
    val window = keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))

    window.sum(1).print()

    env.execute("SessionWindow")
  }
}
/**
 * a,1
 * res:(a,1) 有系统时间
 * a,1
 * a,1
 * a,1
 * res:(a,3)
 */