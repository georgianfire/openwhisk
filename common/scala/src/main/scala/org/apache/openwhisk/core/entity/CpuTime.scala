package org.apache.openwhisk.core.entity

case class CpuTime(milliCpus: Int) {
  require(milliCpus > 0, "Cpu time must be positive")

  def toCpuShares: Int = math.floor(milliCpus.doubleValue / 1000 * 1024).toInt

  def toCfsQuotaAndPeriod: (Int, Int) = (milliCpus * 100, 100000)
  /**
   * TODO: add methods to convert to cpu shares and quota/period for docker
   * references:
   * 1. https://docs.docker.com/config/containers/resource_constraints/
   * 2. https://medium.com/@betz.mark/understanding-resource-limits-in-kubernetes-cpu-time-9eff74d3161b
   */
}