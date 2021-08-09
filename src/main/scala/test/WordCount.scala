package test

import org.apache.flink.api.scala._

/**
  * Copyright (C) 2017 - 2018 Mobike Inc., All Rights Reserved.
  *
  * @author: chenshangan#mobike.com
  * @date: 2018-12-24
  */
object WordCount {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val data = List("hi","how are you","hi")
    val ds = env.fromCollection(data)

    val counts = ds.flatMap(_.toLowerCase.split("\\W+").filter(_.nonEmpty))
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    val a = ds.flatMap(_.toLowerCase.split("\\W+").filter(_.nonEmpty))
      .map { (_, 1) }
      .groupBy(0)

    counts.print()
  }

}
