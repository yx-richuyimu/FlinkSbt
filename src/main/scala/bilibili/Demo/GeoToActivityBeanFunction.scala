package bilibili.Demo

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils

class GeoToActivityBeanFunction extends RichMapFunction[String,ActivityBean]{
  private var httpClient:CloseableHttpClient = null
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)

    httpClient = HttpClients.createDefault()
  }

  override def map(line: String): ActivityBean = {
    val fields = line.trim.split(",")

    val uid = fields(0)
    val aid = fields(1)
    val time = fields(2)
    val eventType = fields(3).toInt
    val lng = fields(4).toDouble
    val lat = fields(5).toDouble

    val url = s"https://restapi.amap.com/v3/geocode/regeo?output=xml&location=$lng,$lat&key=<用户的key>&radius=1000&extensions=all"
    var province:String = null
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
    try {
      val status = response.getStatusLine.getStatusCode
      if(status == 200){
        //获取请求的json字符串
        val result = EntityUtils.toString(response.getEntity)

        println(result)
        //转换为json对象
        val jsonObj = JSON.parseObject(result)
        //获取位置信息
        val regeocode = jsonObj.getJSONObject("regeocode")

        if(regeocode != null && !regeocode.isEmpty){
          val address = regeocode.getJSONObject("addressComponent")
          //获取省份
          province = address.getString("province")
        }
      }
    } finally {
      response.close()
    }
    ActivityBean(uid, aid, "", time, eventType, province, lng, lat)
  }

  override def close(): Unit = {
    super.close()
    httpClient.close()
  }
}
