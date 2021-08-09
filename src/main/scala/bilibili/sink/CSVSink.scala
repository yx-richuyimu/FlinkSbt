package bilibili.sink

import org.apache.flink.streaming.api.scala._

object CSVSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.socketTextStream("yangxu-virtual-machine", 8888)

    lines.writeAsText("")

    //writeAsCsv必须是tuple格式
    lines.map((_,1))
      .writeAsCsv("")

    env.execute("CSVSink")
  }
}
