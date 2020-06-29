package system.edge

import java.io.{BufferedWriter, File, FileWriter}

import common.rest.WskRestOperations
import common._
import org.apache.openwhisk.core.entity.CpuTime
import org.apache.openwhisk.core.entity.size._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import spray.json.JsNumber
import spray.json._


@RunWith(classOf[JUnitRunner])
class SizeEffectTests extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll {
  val wskOperations: WskRestOperations = new WskRestOperations
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  implicit val user = getAdditionalTestSubject("sizeTestUser")

  override def afterAll() = {
    disposeAdditionalTestSubject(user.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400"))

  it should "create an action with default weight" in withAssetCleaner(user) { (wp, assetHelper) =>
    val cpu = CpuTime(milliCpus = 500)
    val memory = 256.MB
    val name = s"prime-${cpu.milliCpus}"

    val creationResult = assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      // action.create(name, None, docker = Some("binw/mnv3-action"), cpu = Some(cpu), memory = Some(memory))
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), cpu = Some(cpu), memory = Some(memory))
    }

    val times = 500
    val activationIds = 1 to times map { _ =>
      val runResult = wskOperations.action.invoke(name, parameters = Map("n" -> JsNumber(5000)))
      // Thread.sleep(1000)
      wskOperations.activation.extractActivationId(runResult)
    }

    val activationResults: Seq[Either[String, JsObject]] = activationIds.map { activationIdOption =>
      val activationId = activationIdOption.get
      wskOperations.activation.waitForActivation(activationId)
    }

    // write activation results to file for further analysis
    val outputFolderName = "output"
    val outputFolder = new File(outputFolderName).getAbsoluteFile
    if (!outputFolder.exists()) outputFolder.mkdir()
    val outputFile = new File(s"$outputFolderName/$name.txt").getAbsoluteFile
    if (outputFile.exists()) outputFile.delete()
    if (outputFile.createNewFile()) println("File created")
    val fileWriter = new FileWriter(outputFile)
    val bufferedWriter = new BufferedWriter(fileWriter)
    activationResults.map(_.right.get.compactPrint + "\n").foreach(bufferedWriter.write)
    bufferedWriter.flush()
    bufferedWriter.close()
    fileWriter.close()

    activationResults.filter(_.isLeft).map(_.left.get).foreach(println)
    activationResults.count(_.isRight) should equal(times)
  }

}

