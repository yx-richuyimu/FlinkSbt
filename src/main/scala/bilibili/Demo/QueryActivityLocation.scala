package bilibili.Demo

import bilibili.util.FlinkUtilsV1
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

object QueryActivityLocation {
  def main(args: Array[String]): Unit = {
    val lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema())
    val beans = lines.map(new GeoToActivityBeanFunction())
    beans.print()

    FlinkUtilsV1.getEnv().execute("QueryActivityLocation")
  }
}