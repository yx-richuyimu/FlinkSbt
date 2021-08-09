package bilibili.window

import org.apache.flink.streaming.api.scala._

object CountWindowAll {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    val nums = lines.map(_.toInt)
    //不分组,将整体当成一个组
    val window  = nums.countWindowAll(5)

    val summed = window.sum(0)
    summed.print()

    env.execute("CountWindowAll")
  }
}

/**
 * 2,3,5,1,4,1,1,1,1,1
 * res:
 * 15
 * 5
 */