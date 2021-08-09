package bilibili.Demo.async

import bilibili.Demo.ActivityBean
import com.alibaba.fastjson.JSON
import org.apache.flink.configuration.Configuration
import org.apache.flink.shaded.curator4.com.google.common.base.Supplier
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.nio.client.{CloseableHttpAsyncClient, HttpAsyncClients}
import org.apache.http.util.EntityUtils

import java.util.concurrent.CompletableFuture

//异步
class AsyncGeoToActivityBeanFunction extends RichAsyncFunction[String, ActivityBean]{
  private var httpClient:CloseableHttpAsyncClient = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    //初始化异步的HttpClient
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

  override def asyncInvoke(line: String, resultFuture: ResultFuture[ActivityBean]): Unit = {
    val fields = line.trim.split(",")

    val uid = fields(0)
    val aid = fields(1)
    val time = fields(2)
    val eventType = fields(3).toInt
    val lng = fields(4).toDouble
    val lat = fields(5).toDouble
    val url = s"https://restapi.amap.com/v3/geocode/regeo?output=xml&location=$lng,$lat&key=<用户的key>&radius=1000&extensions=all"
    val httpGet = new HttpGet(url)

    val future = httpClient.execute(httpGet, null)

    CompletableFuture.supplyAsync(new Supplier[ActivityBean] {
      override def get(): String = {
        try {
          val response = future.get()
          var province = ""
          if(response.getStatusLine.getStatusCode == 200){
            val result = EntityUtils.toString(response.getEntity)
            //转换为json对象
            val jsonObj = JSON.parseObject(result)
            //获取位置信息
            val regeocode = jsonObj.getJSONObject("regeocode")

            if(regeocode != null && !regeocode.isEmpty) {
              val address = regeocode.getJSONObject("addressComponent")
              //获取省份
              province = address.getString("province")
            }
          }
            province
        }
      }
    }).thenAccept {
      case province: String => {
        resultFuture.complete(
          Iterable(
            ActivityBean(uid, aid, "", time, eventType, province, lng, lat)
          ))
      }
      case _ => null
    }
  }

  override def close(): Unit = {
    super.close()
    httpClient.close()
  }
}
