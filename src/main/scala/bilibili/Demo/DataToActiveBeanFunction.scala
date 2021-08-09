package bilibili.Demo

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration

import java.sql.{Connection, DriverManager}

class DataToActiveBeanFunction extends RichMapFunction[String,ActivityBean]{
  private var connection: Connection = null
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    //创建MySQL连接
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8","root","pass")
  }

  override def map(line: String): ActivityBean = {
    val fields = line.trim.split(",")

    val uid = fields(0)
    val aid = fields(1)

    // 根据aid作为查询条件查询出name
    val preparedStatement = connection.prepareStatement("SELECT name FROM table WHERE aid = ?")
    preparedStatement.setString(1, aid)
    val resultSet = preparedStatement.executeQuery()
    var name = ""
    while (resultSet.next()){
      name = resultSet.getString(1)
    }
    preparedStatement.close()

    val time = fields(2)
    val eventType = fields(3).toInt
    val province = fields(4)

    ActivityBean(uid, aid, activityName = name, time, eventType, province)
  }

  override def close(): Unit = {
    super.close()
    connection.close()
  }
}
