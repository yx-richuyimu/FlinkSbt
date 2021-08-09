package bilibili.source.realtime

import org.apache.flink.streaming.api.scala._

/**
  * 并行度为1的Source
  */
object SourceDemo1 {
  def main(args: Array[String]): Unit = {
    // 实时计算，创建执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    // 创建抽象的数据集【创建原始抽象数据集的方法：Source】
    // DataStream是一个抽象的数据集
    //val source: DataStream[String] = env.socketTextStream("localhost", 8888)

    // 将客户端的集合并行化成一个抽象的数据集，通常用于测试和实验
    // fromElements是一个有界数据量，虽然是实时程序，但是数据处理完，程序就会退出
    //val nums: DataStream[Int] = env.fromElements(1,2,3,4,5)//相当与spark.parallelize

    // socketTextStream,fromElements,fromCollection并行度为1的Source
    val nums: DataStream[Int] = env.fromCollection(List(1,2,3,4,5,6,7,8,9))
    // 获取DataStream的并行度
    println(nums.getParallelism)

    val filtered: DataStream[Int] = nums.filter(_%2 == 0)
      //.setParallelism(900) //设置并行度
    println(filtered.getParallelism)

    filtered.print()

    env.execute("SourceDemo1")
  }
}
