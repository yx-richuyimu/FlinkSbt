package bilibili.source.realtime

import java.lang

import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.NumberSequenceIterator

/**
  * 可以并行的Source
  */
object SourceDemo2 {
  def main(args: Array[String]): Unit = {
    // 实时计算，创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //val nums: DataStream[lang.Long] = env.fromParallelCollection(new NumberSequenceIterator(1, 10))
    val nums: DataStream[Long] = env.generateSequence(1, 10)
    // 获取DataStream的并行度
    println(nums.getParallelism)

    val filtered: DataStream[Long] = nums.filter(_%2 == 0)
    println(filtered.getParallelism)

    filtered.print()

    env.execute("SourceDemo2")
  }
}
