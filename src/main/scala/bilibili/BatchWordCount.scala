package bilibili

import org.apache.flink.api.scala._

/**
  * 离线word count
  */
object BatchWordCount {
  def main(args: Array[String]): Unit = {
    // 用于离线计算的env
    val env = ExecutionEnvironment.getExecutionEnvironment

    // Source
    val line: DataSet[String] = env.readTextFile("/home/yangxu/test/flink-test/word_count_source.txt")

    // Transformation开始
    val words: DataSet[String] = line.flatMap(_.split(","))

    val wordAndOne: DataSet[(String, Int)] = words.map((_, 1))

    val grouped: GroupedDataSet[(String, Int)] = wordAndOne.groupBy(0)

    val summed: AggregateDataSet[(String, Int)] = grouped.sum(1)
    // Transformation结束

    // 调用Sink
    //summed.writeAsText("")
    summed.print()

    // 执行(
    // 异常的原因就是说，自上次执行以来，没有定义新的数据接收器。
    // 对于离线批处理的算子，如：“count()”、“collect()”或“print()”等既有sink功能，还有触发的功能。
    // 我们上面调用了print()方法，会自动触发execute，所以最后面的一行执行器没有数据可以执行。)
    // env.execute("BatchWordCount")

  }
}
