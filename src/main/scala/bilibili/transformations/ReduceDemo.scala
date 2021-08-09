package bilibili.transformations

import org.apache.flink.streaming.api.scala._

object ReduceDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("yangxu-virtual-machine", 8888)

    lines.flatMap(_.split(" ")).map((_,1))
      .keyBy(_._1)
      .reduce{
        (x, y) =>
          // x,y key相同
          (x._1, x._2 + y._2)
      }
      .print()

    env.execute("ReduceDemo1")
  }
}
