package system.edge

import common.rest.WskRestOperations
import common._
import org.apache.openwhisk.core.entity.CpuTime
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import spray.json.JsNumber
import spray.json._


@RunWith(classOf[JUnitRunner])
class SizeEffectTests extends TestHelpers with WskTestHelpers with WskActorSystem {
  val wskOperations: WskRestOperations = new WskRestOperations
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  implicit val user = getAdditionalTestSubject("sizeTestUser")

  wskadmin.cli(
    Seq("limits", "set", user.namespace,
      "--invocationsPerMinute",  "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400"))

  it should "create an action with default weight" in withAssetCleaner(user) { (wp, assetHelper) =>
    val name = "size test"
    val cpuLimit = CpuTime(milliCpus = 1000)

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), cpu = Some(cpuLimit))
    }

    val activationIds = 1 to 100 map { _ =>
      val runResult = wskOperations.action.invoke(name, parameters = Map("n" -> JsNumber(5000)))
      wskOperations.activation.extractActivationId(runResult)
    }

    activationIds.foreach{ activationIdOption =>
      activationIdOption.foreach { activationId =>
        wskOperations.activation.waitForActivation(activationId) match {
          case Left(value) => println(value)
          case Right(value) =>
            println(activationId)
            println(value.fields("annotations"))
        }
      }
    }
  }
}
