package org.apache.openwhisk.core.entity

import org.apache.openwhisk.core.ConfigKeys
import pureconfig.loadConfigOrThrow
import pureconfig.generic.auto._
import spray.json._

protected[entity] case class WeightConfig(min: Int, max: Int, std: Int)

case class Weight(weightValue: Int) {
  require(Weight.MIN_WEIGHT to Weight.MAX_WEIGHT contains weightValue, s"Weight should be in range [${Weight.MIN_WEIGHT}, ${Weight.MAX_WEIGHT}], received $weightValue")
}

object Weight {
  val config = loadConfigOrThrow[WeightConfig](ConfigKeys.weight)

  protected[core] val MIN_WEIGHT: Int = config.min
  protected[core] val MAX_WEIGHT: Int = config.max
  protected[core] val STD_WEIGHT: Int = config.std

  val defaultWeightValue = 8

  def apply(): Weight = Weight(STD_WEIGHT)

  implicit val serdes: RootJsonFormat[Weight] = new RootJsonFormat[Weight] {
    override def read(json: JsValue): Weight = json match {
      case JsNumber(weightValue) =>
        if (weightValue.isValidInt) Weight(weightValue.toInt)
        else throw DeserializationException(s"Expected an integer value, a non-integer value $weightValue was given instead")
      case somethingElse => throw DeserializationException(s"Expected a number value, $somethingElse was given instead")
    }

    override def write(obj: Weight): JsValue = JsNumber(obj.weightValue)
  }
}
