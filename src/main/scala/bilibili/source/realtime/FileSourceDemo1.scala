package bilibili.source.realtime

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object FileSourceDemo1 {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val lines: DataStream[String] = env.readTextFile("/home/yangxu/test/flink-test/word_count_source.txt")
    println(lines.getParallelism)

    val words: DataStream[String] = lines.flatMap(_.split(","))

    val wordAndOne: DataStream[(String, Int)] = words.map((_, 1))

    val grouped: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)

    val summed: DataStream[(String, Int)] = grouped.sum(1)

    summed.print()

    env.execute("FileSourceDemo1")
  }
}
