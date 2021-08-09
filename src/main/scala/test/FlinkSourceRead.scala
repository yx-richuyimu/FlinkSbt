package test

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}

object FlinkSourceRead {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val env1 = StreamExecutionEnvironment.getExecutionEnvironment


    // environment configuration
    val settings = EnvironmentSettings
      .newInstance()
      .inStreamingMode()
      .build();

    val tEnv = TableEnvironment.create(settings)

    tEnv.sqlQuery("")

    case class Event(i: Int, str: String, d: Double)
    val data = env.fromElements(
      Event(1, "barfoo", 1.0),
      Event(2, "start", 2.0),
      Event(3, "foobar", 3.0)
    )

    env.readTextFile("")
    env1.readTextFile("")
  }
}
