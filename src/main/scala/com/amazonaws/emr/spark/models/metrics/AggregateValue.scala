package com.amazonaws.emr.spark.models.metrics

class AggregateValue {

  var samples: Long = 0L
  var value: Long = 0L
  var m2: Double = 0.0
  var min: Long = Long.MaxValue
  var max: Long = Long.MinValue
  var mean: Double = 0.0
  var variance: Double = 0.0

  override def toString: String = {
    s"""{
       | "value": ${value},
       | "min": ${min},
       | "max": ${max},
       | "mean": ${mean},
       | "variance": ${variance}
       }""".stripMargin
  }

}