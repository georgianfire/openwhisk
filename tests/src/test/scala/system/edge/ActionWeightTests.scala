package system.edge

import common._
import common.rest.WskRestOperations
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ActionWeightTests extends TestHelpers with WskTestHelpers with WskActorSystem {
  implicit val wskProps: WskProps = WskProps()
  val wskOperations: WskOperations = new WskRestOperations

  behavior of "Action weight"

  it should "create an action with default weight" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "default weight"

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")))
    }

    creationResult.stdout should include("\"weight\":8") // Standard weight is 8
    // wskOperations.action.invoke(name)
  }

  it should "create an action with explicit weight" in withAssetCleaner(wskProps) { (wp, asseetHelper) =>
    val name = "explicit weight"
    val weight = 12

    val creationResult = asseetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")), weight = Some(weight))
    }

    creationResult.stdout should include("\"weight\":" + weight)
  }

  it should "update an action with new weight" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "weight update"
    val initialWeight = 16
    val updatedWeight = 32

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")), weight = Some(initialWeight))
    }

    creationResult.stdout should include("\"weight\":" + initialWeight)

    val updateResult = wskOperations.action.create(name, None , update = true, weight = Some(updatedWeight))

    updateResult.stdout should include("\"weight\":" + updatedWeight)

    val getResult = wskOperations.action.get(name)

    getResult.stdout should include("\"weight\":" + updatedWeight)

    println(getResult.stdout)
  }
}
