package bilibili.window


import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import java.util.concurrent.TimeUnit
/**
 * 分组后再调用TumblingWindow,每一个组达到一定的条数才会触发任务执行
 */
object TumblingWindow {
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
    val window = keyed.timeWindow(Time.seconds(5))
    //相当于
    //keyed.window(TumblingProcessingTimeWindows.of(Time.of(5,TimeUnit.SECONDS)))
    window.sum(1).print()

    env.execute("TumblingWindow")
  }
}
/**
 * a,1
 * b,1
 * res:(b,1)(a,1)
 * a,1
 * a,1
 * a,1
 * res:(a,3)
 * c,1
 * c,1
 * res:(c,2)
 */