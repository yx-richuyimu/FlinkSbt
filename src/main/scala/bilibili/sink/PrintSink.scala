package bilibili.sink

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object PrintSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("yangxu-virtual-machine", 8888)

    //return
    //res:1>aaa
    //res:2>bbb
    lines.print("res").setParallelism(2)
    env.execute("PrintSink")
  }
}
