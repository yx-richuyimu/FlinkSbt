package bilibili

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

/**
  * 实时word count
  */
object StreamWordCount {

  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    //Transformation开始
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    val wordAndOne: DataStream[(String, Int)] =  words.map((_, 1))

    val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)

    val summed: DataStream[(String, Int)] = keyed.sum(1)
    //Transformation结束

    //调用Sink
    summed.print()

    //执行
    env.execute("StreamWordCount")
  }
}
