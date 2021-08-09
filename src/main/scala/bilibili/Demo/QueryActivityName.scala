package bilibili.Demo

import bilibili.util.FlinkUtilsV1
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._

object QueryActivityName {
  def main(args: Array[String]): Unit = {
    val lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema())
    val beans = lines.map(new DataToActiveBeanFunction())
    beans.print()

    FlinkUtilsV1.getEnv().execute("QueryActivityName")
  }
}
case class ActivityBean(uid: String,
                        aid: String,
                        activityName: String,
                        time: String,
                        eventType: Int,
                        province: String,
                        lng: Double = 0.0,
                        lat: Double = 0.0,
                        count: Int = 1)