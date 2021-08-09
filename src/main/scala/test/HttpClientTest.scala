package test

import com.alibaba.fastjson.JSON
import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils



object HttpClientTest {
  def main(args: Array[String]): Unit = {
    val lng = 116.31
    val lat = 40.028

    val url = s"https://restapi.amap.com/v3/geocode/regeo?output=xml&location=$lng,$lat&key=<用户的key>&radius=1000&extensions=all"
    val httpClient = HttpClients.createDefault()
    val httpGet = new HttpGet(url)
    val response = httpClient.execute(httpGet)
    try{
      val status = response.getStatusLine.getStatusCode
      var province:String = null
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
      println(province)
    }finally {
      response.close()
    }
  }
}
