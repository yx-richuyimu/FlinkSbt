package bilibili.Demo.async.request

import com.alibaba.druid.pool.DruidDataSource
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator4.com.google.common.base.Supplier
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.concurrent.{CompletableFuture, ExecutorService, Executors, Future}

//异步
class AsyncMySQLRequest extends RichAsyncFunction[String, String]{
  // 连接池
  private var dataSource:DruidDataSource = null
  // 线程池
  private var executorService: ExecutorService = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    executorService = Executors.newFixedThreadPool(20)

    dataSource = new DruidDataSource()
    dataSource.setDriverClassName("com.mysql.jdbc.Driver")
    dataSource.setUsername("root")
    dataSource.setPassword("123456")
    dataSource.setUrl("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8")
    dataSource.setInitialSize(5)
    dataSource.setMinIdle(10)
    dataSource.setMaxActive(20)
  }

  override def asyncInvoke(id: String, resultFuture: ResultFuture[String]): Unit = {
    // 将查询功能放入线程池中
    // submit有返回,execute没有返回值的
    val future: Future[String] = executorService.submit{
      case _ => {
      queryFromMysql(id)
    }
    }

    CompletableFuture.supplyAsync(new Supplier[String] {
      override def get(): String = {
        try{
          future.get()
        }catch {
          case ex:Exception => null
        }
      }
    }).thenAccept{
      case dbResult: String => resultFuture.complete(Iterable(dbResult))
      case _ => null
    }
  }

  override def close(): Unit = {
    super.close()
    executorService.shutdown()
  }

  def queryFromMysql(param: String): String ={
    val sql = "SELECT id,name FROM table WHERE id = ?"
    var res = ""

    var connection: Connection = null
    var stmt: PreparedStatement = null
    var rs: ResultSet = null
    try{
      connection = dataSource.getConnection()
      stmt = connection.prepareStatement(sql)
      stmt.setString(1, param)
      rs = stmt.executeQuery()
      while (rs.next()){
        res = rs.getString("name")
      }
    }finally {
      if(rs != null){
        rs.close()
      }
      if(stmt != null){
        stmt.close()
      }
      if(connection != null){
        connection.close()
      }
    }
    if(res != null){
      //放入缓存
    }
    res
  }
}
