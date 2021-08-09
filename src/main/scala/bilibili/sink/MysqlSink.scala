package bilibili.sink

import bilibili.Demo.ActivityBean
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

import java.sql.{Connection, DriverManager}

class MysqlSink extends RichSinkFunction[ActivityBean]{
  private var connection: Connection = null
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 创建Mysql连接
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","pass")
  }

  override def close(): Unit = {
    super.close()
    //关闭连接
    connection.close()
  }

  override def invoke(value: ActivityBean, context: SinkFunction.Context): Unit = {
    super.invoke(value, context)

    // 当key存在就进行更新
    val pstm = connection.prepareStatement("insert into table(aid, eventTime, count) values() on duplicate key update counts = ?")
    pstm.setString(1, value.aid)

    pstm.executeLargeUpdate()
    pstm.close()
  }
}
