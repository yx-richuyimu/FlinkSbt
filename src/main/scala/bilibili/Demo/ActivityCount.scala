package bilibili.Demo

import bilibili.Demo.async.AsyncGeoToActivityBeanFunction
import bilibili.sink.MysqlSink
import bilibili.util.FlinkUtilsV1
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

import java.util.concurrent.TimeUnit

object ActivityCount {
  def main(args: Array[String]): Unit = {
    val lines = FlinkUtilsV1.createKafkaStream(args, new SimpleStringSchema())

    val beans = AsyncDataStream.unorderedWait(
      lines,
      new AsyncGeoToActivityBeanFunction,
      0,
      TimeUnit.MICROSECONDS,
      10
    )

    val summed1 = beans.keyBy("aid","eventType").sum("count")
    val summed2 = beans.keyBy("aid","eventTYpe", "province").sum("count")

    //调用Sink
    summed1.addSink(new MysqlSink)

    //创建Redis Conf
    val conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPassword("").build()
    summed2.addSink(new RedisSink[ActivityBean](conf, new RedisActivityBeanMapper))
    //执行
    FlinkUtilsV1.getEnv().execute("ActivityCount")
  }
}

// RedisSink
class RedisActivityBeanMapper extends RedisMapper[ActivityBean]{
  // 调用Redis的写入方法
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "ACT_COUNT")
  }

  // 写入Redis的key
  override def getKeyFromData(data: ActivityBean): String = data.aid + "_" + data.eventType + "_" + data.province

  //写入Redis的Value
  override def getValueFromData(data: ActivityBean): String = data.count.toString
}