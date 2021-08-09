package bilibili.Demo.async.request

import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator4.com.google.common.base.Supplier
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.TransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

import java.net.InetAddress
import java.util.concurrent.CompletableFuture

class AsyncEsRequest extends RichAsyncFunction[String, (String, String)]{
  // ES本身就支持异步查询
  private var transportClient: TransportClient = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    // 设置集群名称
    val settings = Settings.builder().put("cluster.name", "docker-cluster").build()
    //创建client
    transportClient = new PreBuiltTransportClient(settings).addTransportAddress(
      new TransportAddress(InetAddress.getByName("localhost"), 9300)
    )
  }

  override def close(): Unit = {
    super.close()
    transportClient.close()
  }

  override def asyncInvoke(key: String, resultFuture: ResultFuture[(String, String)]): Unit = {
    val actionFuture = transportClient.get(new GetRequest("数据库", "表", "条件"))

    CompletableFuture.supplyAsync(new Supplier[String] {

      override def get(): String = {
        try{
          val response = actionFuture.get()
          response.getSource.get("name").toString
        }catch {
          case ex: Exception => null
        }
      }
    }).thenAccept{
      case dbResult: String => resultFuture.complete(Iterable((key, dbResult)))
    }
  }
}
