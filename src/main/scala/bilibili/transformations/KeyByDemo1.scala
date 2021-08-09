package bilibili.transformations

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala._

object KeyByDemo1 {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //直接输入的就是单词
    val words = env.socketTextStream("yangxu-virtual-machine", 8888)

    //分组
    //val wordAndOne = words.flatMap(_.split(" ")).map((_,1))
    //在JAVA中认为元组也是一个特殊的元组,下标从0开始
    // [(String, Int), String] [分组后的结果, key的类型按哪个key分组的]
    //val keyed: KeyedStream[(String, Int), String] = wordAndOne.keyBy(_._1)
    //val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)
    /*两中打印结果一致
      1> (spark,1)
      1> (spark,1)
      2> (hue,1)
      2> (hue,1)
      1> (hbase,1)
      2> (hadoop,1)
     */

    val wordAndOne = words.flatMap(_.split(" ")).map(WordCounts(_, 1))
    val keyed: KeyedStream[WordCounts, String] = wordAndOne.keyBy(_.word)
    //val keyed: KeyedStream[WordCounts, Tuple] = wordAndOne.keyBy("word")
    //keyed.print()
    //聚合
    keyed.sum("counts").print()

    env.execute("KeyByDemo1")
  }
}
case class WordCounts(word: String,
                     counts: Int)