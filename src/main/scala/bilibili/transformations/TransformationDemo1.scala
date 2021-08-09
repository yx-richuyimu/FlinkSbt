package bilibili.transformations

import org.apache.flink.streaming.api.scala._

/**
 * 对DataStream进行操作,返回一个新的DataStream
 */
object TransformationDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val lines = env.generateSequence(0, 10)
    //transform
    val newLines = lines
      .filter(_%2 == 0).map(_ * 2)

/*    val lines = env.fromElements("spark flink hadoop", "hive spark python scala","google chrome")
    //sink
    val newLines = lines.flatMap(_.split(" "))*/
    newLines.print()

    env.execute("TransformationDemo1")
  }
}
