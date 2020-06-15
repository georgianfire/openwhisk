package system.edge

import common._
import common.rest.WskRestOperations
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class UserWeightTests extends TestHelpers with WskTestHelpers with WskActorSystem{
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  val userName = "userWithWeight"
  implicit val user = getAdditionalTestSubject(userName)
  val wskOperations: WskOperations = new WskRestOperations
  val actionName = "weight test"

  behavior of "User weight"

  it should "set Weight for user" in withAssetCleaner(user) { (wp, assetHelper) =>
    val weight = 16

    wskadmin.cli(
      Seq(
        "limits",
        "set",
        user.namespace,
        "--weight",
        s"$weight"
      ))

    val result = wskadmin.cli(
      Seq(
        "limits",
        "get",
        user.namespace
      ))

    result.stdout should include(s"weight = $weight")

//    val invocationResult = assetHelper.withCleaner(wskOperations.action, actionName) { (action, _) =>
//      import spray.json._
//      import spray.json.DefaultJsonProtocol._
//      action.create(actionName, Some(TestUtils.getTestActionFilename("hello.py")))
//      action.invoke(actionName, parameters = Map("name" -> "edgewhisk".toJson))
//    }
//
//    println(invocationResult.stdout)
  }
}
