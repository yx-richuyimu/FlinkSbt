 name := "flink-sbt"
 
  version := "1.0"
 
  scalaVersion := "2.11.11"
  updateOptions := updateOptions.value.withCachedResolution(true)
  libraryDependencies ++= Seq(
    "org.apache.flink" %% "flink-clients" % "1.10.1",
    "org.apache.flink" %% "flink-scala" % "1.10.1",
    "org.apache.flink" %% "flink-streaming-scala" % "1.10.1",
    "org.apache.flink" %% "flink-connector-kafka" % "1.10.1",
    "org.apache.flink" % "flink-table-api-java-bridge_2.11" % "1.13.0",
    "org.apache.flink" % "flink-table-common" % "1.13.0",
    "org.apache.httpcomponents" % "httpclient" % "4.5.9", // 网络接口
    "org.apache.httpcomponents" % "httpasyncclient" % "4.1.4", // 网络接口
    "com.alibaba" % "fastjson" % "1.2.47", // json转换
    "com.alibaba" % "druid" % "1.0.23", // 数据库连接池,线程池
    "org.elasticsearch" % "elasticsearch" % "7.1.0", //ES连接
    "org.elasticsearch.client" % "transport" % "7.1.0",  //ES连接
    "org.elasticsearch.plugin" % "transport-netty4-client" % "7.1.0" //ES连接
    //"org.apache.bahir" % "flink-connector-redis_2.11" % "1.0" //redis连接
  )
