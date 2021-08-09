package bilibili.transformations

import org.apache.flink.streaming.api.scala._

object MaxMinDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("yangxu-virtual-machine", 8888)


    lines.map(x => (x.split(",")(0), x.split(",")(1).toInt))
      .keyBy(_._1)
      .max(1)
      .print()

    env.execute("MaxDemo")
  }
}
