package bilibili.sink

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object AddSinkDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("yangxu-virtual-machine", 8888)

    // 可以定义mysql/redis等链接写入
    lines.addSink(x => println(x))

    env.execute("AddSinkDemo")
  }
}
