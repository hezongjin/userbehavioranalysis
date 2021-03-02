package com.dadiyunwu.hotitems_analysis.util

import java.util

import org.apache.flink.runtime.instance.SlotSharingGroupId
import org.apache.flink.runtime.jobgraph.{JobGraph, SavepointRestoreSettings}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.graph.StreamGraph
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.test.util.MiniClusterWithClientResource

object FlinkHelper {


  def start(env: StreamExecutionEnvironment, ckPath: String) = {
    val externalChekpoint = ckPath
    run(env.getStreamGraph, externalChekpoint)
  }

  def run(streamGraph: StreamGraph, externalChekpoint: String) = {

    val jobGraph = streamGraph.getJobGraph()
    if (externalChekpoint != null) {
      jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(externalChekpoint))
    }

    //计算 jobGraph需要的slot个数
    val slotNum = getSlotNum(jobGraph)

    import org.apache.flink.client.program.ClusterClient
    // 初始化 MiniCluster
    val clusterClient = initCluster(slotNum)
    // 提交任务到 MiniCluster
    clusterClient.submitJob(jobGraph)

  }

  private def getSlotNum(jobGraph: JobGraph): Int = { // 保存每个 SlotSharingGroup 需要的 slot 个数
    val map: util.HashMap[SlotSharingGroupId, Integer] = new util.HashMap[SlotSharingGroupId, Integer]
    import scala.collection.JavaConversions._
    for (jobVertex <- jobGraph.getVertices) {
      val slotSharingGroupId: SlotSharingGroupId = jobVertex.getSlotSharingGroup.getSlotSharingGroupId
      val parallelism: Int = jobVertex.getParallelism
      val oldParallelism: Int = map.getOrDefault(slotSharingGroupId, 0)
      if (parallelism > oldParallelism) map.put(slotSharingGroupId, parallelism)
    }
    // 将所有 SlotSharingGroup 的 slot 个数累加
    var slotNum: Int = 0
    import scala.collection.JavaConversions._
    for (parallelism <- map.values) {
      slotNum += parallelism
    }
    slotNum
  }

  @throws[Exception]
  private def initCluster(slotNum: Int) = {
    val cluster = new MiniClusterWithClientResource(new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(slotNum).build)
    cluster.before()
    cluster.getClusterClient
  }
}
