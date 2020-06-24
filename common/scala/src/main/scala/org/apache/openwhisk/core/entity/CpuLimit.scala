package org.apache.openwhisk.core.entity

import org.apache.openwhisk.core.ConfigKeys
import pureconfig.loadConfigOrThrow
import pureconfig.generic.auto._
import spray.json._

import scala.util.{Failure, Success, Try}

protected[entity] case class CpuLimitConfig(min: CpuTime, max: CpuTime, std: CpuTime)

class CpuLimit private (val cpuTime: CpuTime) extends AnyVal
object CpuLimit {
  val config: CpuLimitConfig = loadConfigOrThrow[CpuLimitConfig](ConfigKeys.cpu)

  protected[core] val MIN_CPU: CpuTime = config.min
  protected[core] val MAX_CPU: CpuTime = config.max
  protected[core] val STD_CPU: CpuTime = config.std

  protected[core] val standardCpuLimit: CpuLimit = CpuLimit(STD_CPU)

  protected[core] def apply(): CpuLimit = standardCpuLimit

  @throws[IllegalArgumentException]
  protected[core] def apply(cpuTime: CpuTime): CpuLimit = {
    require(cpuTime >= MIN_CPU, s"cpu $cpuTime below allowed threshold $MIN_CPU")
    require(cpuTime <= MAX_CPU, s"cpu $cpuTime above allowed threshold $MAX_CPU")
    new CpuLimit(cpuTime)
  }

  implicit val serdes: RootJsonFormat[CpuLimit] = new RootJsonFormat[CpuLimit] {
    override def write(obj: CpuLimit): JsValue = JsNumber(obj.cpuTime.milliCpus)

    override def read(json: JsValue): CpuLimit =
      Try {
        val JsNumber(milliCpus) = json
        require(milliCpus.isWhole)
        CpuLimit(CpuTime(milliCpus.intValue))
      } match {
        case Success(cpuLimit) => cpuLimit
        case Failure(e: IllegalArgumentException) => deserializationError(e.getMessage, e)
        case Failure(e: Throwable) => deserializationError(e.getMessage, e)
      }
  }
}
