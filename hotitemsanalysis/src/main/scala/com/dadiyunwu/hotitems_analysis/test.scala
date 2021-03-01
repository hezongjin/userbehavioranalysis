package com.dadiyunwu.hotitems_analysis

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.configuration.{ConfigOption, Configuration}
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala._

object test {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 1000L))

    val stateBackend: StateBackend = new RocksDBStateBackend("file:///D:\\MyCk", true)
    env.setStateBackend(stateBackend)

    env.enableCheckpointing(10 * 1000L)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointTimeout(60 * 1000L)
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.setTolerableCheckpointFailureNumber(3)

    val inputStream = env.readTextFile(this.getClass.getClassLoader.getResource("file.csv").getPath)

    inputStream.map(data => {
      val arr = data.split(",")
      UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
    })
      .print()


    env.execute()
  }
}
