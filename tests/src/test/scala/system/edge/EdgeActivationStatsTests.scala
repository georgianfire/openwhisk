package system.edge

import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import common._
import common.rest.WskRestOperations

@RunWith(classOf[JUnitRunner])
class EdgeActivationStatsTests extends TestHelpers with WskTestHelpers with JsHelpers with WskActorSystem{
  implicit val wskProps = WskProps()
  val wsk: WskOperations = new WskRestOperations

  val guestNamespace = wskProps.namespace

  behavior of "Whisk controller"

  it should "returns activation stats within a time window" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "hello stats"

    assetHelper.withCleaner(wsk.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")))
    }

    1 to 5 foreach { _ =>
      val run = wsk.action.invoke(name)
      withActivation(wsk.activation, run) { activation =>
        activation.response.status shouldBe "success"
      }
    }
  }
}
