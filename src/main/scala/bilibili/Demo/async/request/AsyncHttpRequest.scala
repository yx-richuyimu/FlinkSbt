package bilibili.Demo.async.request

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}

// 异步查询样例
class AsyncHttpRequest extends RichAsyncFunction[String, String]{
  private var httpClient:CloseableHttpAsyncClient = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val requestConfig = RequestConfig.custom()
      .setSocketTimeout(3000)
      .setConnectTimeout(3000)
      .build()
    httpClient = HttpAsyncClients.custom()
      .setMaxConnTotal(20)
      .setDefaultRequestConfig(requestConfig)
      .build()
    httpClient.start()
  }

  override def close(): Unit = {
    super.close()
    httpClient.close()
  }

  override def asyncInvoke(input: String, resultFuture: ResultFuture[String]): Unit = {
    ???
  }
}
