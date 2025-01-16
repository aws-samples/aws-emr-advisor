package com.amazonaws.emr.spark.models.runtime

import com.amazonaws.emr.spark.models.runtime.EmrServerlessEnv.findWorkerByCpuMemory
import com.amazonaws.emr.utils.Formatter.byteStringAsBytes
import org.scalatest.funsuite.AnyFunSuiteLike

class EmrServerlessEnvTest extends AnyFunSuiteLike {

  test("testFindWorkerByCpuMemory") {

    val memory1Gb = byteStringAsBytes("1g")
    val memory2Gb = byteStringAsBytes("2g")
    val memory8Gb = byteStringAsBytes("8g")
    val memory16Gb = byteStringAsBytes("16g")
    val memory32Gb = byteStringAsBytes("32g")
    val memory120Gb = byteStringAsBytes("120g")
    val memory240Gb = byteStringAsBytes("240g")

    // Check min values
    assert(findWorkerByCpuMemory(0, 0L).equals(WorkerNode(1, memory2Gb, 0L)))
    assert(findWorkerByCpuMemory(1, memory1Gb).equals(WorkerNode(1, memory2Gb, 0L)))
    // Check max values
    assert(findWorkerByCpuMemory(64, memory120Gb).equals(WorkerNode(16, memory120Gb, 0L)))
    // Check mixed
    assert(findWorkerByCpuMemory(1, memory16Gb).equals(WorkerNode(2, memory16Gb, 0L)))
    assert(findWorkerByCpuMemory(1, memory32Gb).equals(WorkerNode(8, memory32Gb, 0L)))
    assert(findWorkerByCpuMemory(32, memory32Gb).equals(WorkerNode(16, memory32Gb, 0L)))
    assert(findWorkerByCpuMemory(32, memory240Gb).equals(WorkerNode(16, memory120Gb, 0L)))
    assert(findWorkerByCpuMemory(32, memory1Gb).equals(WorkerNode(16, memory32Gb, 0L)))
    assert(findWorkerByCpuMemory(1, memory240Gb).equals(WorkerNode(16, memory120Gb, 0L)))
    assert(findWorkerByCpuMemory(16, memory2Gb).equals(WorkerNode(16, memory32Gb, 0L)))

  }

}
