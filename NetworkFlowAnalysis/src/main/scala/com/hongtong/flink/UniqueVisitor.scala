package com.hongtong.flink

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object UniqueVisitor {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val stream = env
      .readTextFile("E:\\code\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(line => {
        val linearray = line.split(",")
        UserBehavior(linearray(0).toLong, linearray(1).toLong, linearray(2).toInt, linearray(3), linearray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .timeWindowAll(Time.hours(1))
//      .apply(new UvCountByWindow())   全窗口模式
      .aggregate(new UvCountAgg(),new UvCountResultIncreAgg())  //增量模式
      .print()

    env.execute("Unique Visitor Job")

  }
}

class  UvCountAgg extends AggregateFunction[UserBehavior,Set[Long],Long]{
  override def createAccumulator(): Set[Long] = Set[Long]()

  override def add(value: UserBehavior, accumulator: Set[Long]): Set[Long] = accumulator +value.userId

  override def getResult(accumulator: Set[Long]): Long = accumulator.size

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = a ++ b
}

class UvCountResultIncreAgg extends  AllWindowFunction[Long,UvCount,TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
    out.collect(UvCount(window.getEnd,input.head))
  }
}

class UvCountByWindow extends AllWindowFunction[UserBehavior, UvCount, TimeWindow]{
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    val s: collection.mutable.Set[Long] = collection.mutable.Set()
    var idSet = Set[Long]()

    for ( userBehavior <- input) {
      idSet += userBehavior.userId
    }
    out.collect(UvCount(window.getEnd, idSet.size))

  }
}

case class UvCount(windowEnd: Long, count: Long)