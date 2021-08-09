package bilibili.Demo.async

import bilibili.util.FlinkUtilsV1
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

import java.util.concurrent.TimeUnit

object AsyncQueryActivityLocation {
  def main(args: Array[String]): Unit = {
    val lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema())
    val beans = AsyncDataStream
      .unorderedWait(
        lines,
        new AsyncGeoToActivityBeanFunction,
        3000,
        TimeUnit.MICROSECONDS,
        10)
    beans.print()

    FlinkUtilsV1.getEnv().execute("AsyncQueryActivityLocation")
  }
}
