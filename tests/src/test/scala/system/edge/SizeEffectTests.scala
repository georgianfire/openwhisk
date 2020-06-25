package system.edge

import common.rest.WskRestOperations
import common.{TestHelpers, TestUtils, WskActorSystem, WskOperations, WskProps, WskTestHelpers}
import org.apache.openwhisk.core.entity.CpuTime
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import spray.json.JsNumber
import spray.json._


@RunWith(classOf[JUnitRunner])
class SizeEffectTests extends TestHelpers with WskTestHelpers with WskActorSystem {
  implicit val wskProps: WskProps = WskProps()
  val wskOperations: WskOperations = new WskRestOperations

  it should "create an action with default weight" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "size test"
    val cpuLimit = CpuTime(milliCpus = 500)

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), cpu = Some(cpuLimit))
    }

    withActivation(wskOperations.activation, wskOperations.action.invoke(name, parameters = Map("n" -> JsNumber(10000)))) { activation =>
      val result = wskOperations.activation.get(Some(activation.activationId)).stdout.parseJson.asJsObject
      println(result)
    }
  }
}
