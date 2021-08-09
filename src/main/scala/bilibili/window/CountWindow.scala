package bilibili.window

import org.apache.flink.streaming.api.scala._

/**
 * 分组后再调用CountWindow,每一个组达到一定的条数才会触发任务执行
 */
object CountWindow {
  def main(args: Array[String]): Unit = {
    //实时计算的env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //spark,3
    //hadoop,2
    val lines: DataStream[String] = env.socketTextStream("localhost", 8888)

    val wordAndCount = lines.map(x => (x.split(",")(0), x.split(",")(1).toInt))
    //先分组,再划分窗口
    val keyed = wordAndCount.keyBy(_._1)
    //划分窗口
    val window = keyed.countWindow(5)
    window.sum(1).print()

    env.execute("CountWindow")
  }
}
/*
hadoop,2
hadoop,3
spark,1
spark,2
spark,3
hue,1
hue,2
hue,3
hadoop,1
hadoop,1
hadoop,1
spark,1
spark,1
flink,1
flink,1
flink,1
flink,1
flink,1
hadoop,1
hadoop,1
hadoop,1
hadoop,1
hadoop,1
res:
2> (hadoop,8)
1> (spark,8)
2> (flink,5)
2> (hadoop,5)
 */