package system.edge

import common.rest.WskRestOperations
import common.{TestHelpers, TestUtils, WskActorSystem, WskOperations, WskProps, WskTestHelpers}
import org.apache.openwhisk.core.entity.CpuTime
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ActionCpuLimitTests extends TestHelpers with WskTestHelpers with WskActorSystem {
  implicit val wskProps: WskProps = WskProps()
  val wskOperations: WskOperations = new WskRestOperations()

  behavior of "Action cpu limit"

  it should "create an action with default cpu limit" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "default cpu limit"

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")))
    }

    creationResult.stdout should include("\"cpu\":200")
  }

  it should "create an action with explicit cpu limit" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "explicit cpu limit"
    val cpuLimit = CpuTime(milliCpus = 500)

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")), cpu = Some(cpuLimit))
    }

    creationResult.stdout should include("\"cpu\":500")
  }

  it should "update an action with new cpu limit" in withAssetCleaner(wskProps) { (wp, assetHelper) =>
    val name = "cpu limit update"
    val initialCpuLimit = CpuTime(300)
    val updatedCpuLimit = CpuTime(700)

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("hello.py")), cpu = Some(initialCpuLimit))
    }

    creationResult.stdout should include("\"cpu\":" + initialCpuLimit.milliCpus)

    val updateResult = wskOperations.action.create(name, None , update = true, cpu = Some(updatedCpuLimit))

    updateResult.stdout should include("\"cpu\":" + updatedCpuLimit.milliCpus)

    val getResult = wskOperations.action.get(name)

    getResult.stdout should include("\"cpu\":" + updatedCpuLimit.milliCpus)

    println(getResult.stdout)
  }
}
