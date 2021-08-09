package test

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.{AggregateDataSet, DataSet, GroupedDataSet}
import org.apache.flink.streaming.api.scala._

/**
  * 并行度为1的Source
  */
object StartNewChain {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //Source
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    //Transformation开始
    val words: DataStream[String] = lines.flatMap(_.split(" "))

    // startNewChain从这算子开始看是一个新的task,发生redistributing
    //val wordAndOne: DataStream[(String, Int)] =  words.map((_, 1)).filter(_._1.startsWith("h")).startNewChain()

    //将当前算子单独生成一个task中,不会生成operate chain,在计算复杂算子时,需要内存很多时
    //val wordAndOne: DataStream[(String, Int)] =  words.map((_, 1)).filter(_._1.startsWith("h")).disableChaining()

    //Flink默认资源槽,默认的名字是default
    //对于同一个Job,不同Task(阶段)的subtask可以在同一个资源槽中(默认的方式,Pipeline),前提所有Task的共享资源槽的名字相同
    //不同就不能在同一个资源槽里面了
    /**
     * solt0      solt1     solt2    solt3
     * default    doit      doit     doit
     * source     filter    filter   filter
     *            sink      sink     sink
     *
     */
    val wordAndOne: DataStream[(String, Int)] =  words.map((_, 1)).filter(_._1.startsWith("h")).slotSharingGroup("doit")

    val keyed: KeyedStream[(String, Int), Tuple] = wordAndOne.keyBy(0)

    val summed: DataStream[(String, Int)] = keyed.sum(1)//.slotSharingGroup("default")
    //Transformation结束

    //调用Sink
    summed.print()

    //执行
    env.execute("StreamWordCount")
  }
}
