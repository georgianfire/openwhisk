package org.apache.openwhisk.core.entity

import spray.json._

case class CpuTime(milliCpus: Int) {
  require(milliCpus > 0, "Cpu time must be positive")

  def toCpuShares: Int = math.floor(milliCpus.doubleValue / 1000 * 1024).toInt

  def toCfsQuotaAndPeriod: (Int, Int) = (milliCpus * 100, 100000)
  /**
   * references:
   * 1. https://docs.docker.com/config/containers/resource_constraints/
   * 2. https://medium.com/@betz.mark/understanding-resource-limits-in-kubernetes-cpu-time-9eff74d3161b
   */
}

object CpuTime {
  implicit val serdes: RootJsonFormat[CpuTime] = new RootJsonFormat[CpuTime] {
    override def read(json: JsValue): CpuTime = json match {
      case JsNumber(milliCpus) =>
        if (milliCpus.isValidInt) CpuTime(milliCpus.toInt)
        else throw DeserializationException(s"Expected an integer value, received $milliCpus instead")
      case somethingElse => throw DeserializationException(s"Expected number value, received $somethingElse instead")
    }

    override def write(obj: CpuTime): JsValue = JsNumber(obj.milliCpus)
  }
}
