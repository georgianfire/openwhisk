package system.edge

import java.io.{BufferedWriter, File, FileWriter}
import java.time.Instant

import org.apache.openwhisk.core.entity.CpuTime
import org.apache.openwhisk.core.entity.size._
import common.rest.{RestResult, WskRestOperations}
import common.{TestHelpers, TestUtils, WskActorSystem, WskAdmin, WskTestHelpers}
import org.apache.openwhisk.core.containerpool.Interval
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import spray.json.JsNumber

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class QueueingTests extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll{
  implicit val wskadmin = WskAdmin.kubeWskadmin
  val wskOperations: WskRestOperations = new WskRestOperations
  val name = "prime"
  implicit val user = getAdditionalTestSubject(name)

  override def afterAll(): Unit = {
    disposeAdditionalTestSubject(user.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400"))

  val highRate = 30
  val lowRate = 5
  implicit val rng = new Random()

  def getRate(start: Instant, now: Instant):Double = {
    val minutes = Interval(start, now).duration.toMinutes
    if (minutes < 18) (minutes / 3 + 1) * 5
    else 30 - ((minutes - 18) / 3 + 1) * 5
  }

  def genExponential(lambda: Double)(implicit rng: Random): Double = {
    math.log(1 - rng.nextDouble()) / (-lambda)
  }

  it should "Queueing model" in withAssetCleaner(user) { (wp, assetHelper) =>
    assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), memory = Some(256.MB), cpu = Some(CpuTime(300)))
    }

    val start = Instant.now
    println(start)

    var futures: Seq[Future[RestResult]] = Seq.empty
    while (Interval(start, Instant.now).duration.toMinutes < 33) {
      futures = futures :+ Future {
        val invokeResult = wskOperations.action.invoke(name, parameters = Map("n" -> JsNumber(3000)))
        val activationId = wskOperations.activation.extractActivationId(invokeResult)
        wskOperations.activation.get(activationId)
      }

      Thread.sleep((genExponential(getRate(start, Instant.now)) * 1000).toInt)
    }

    val results = Await.result(Future.sequence(futures), Duration.Inf)

    // write activation results to file for further analysis
    val outputFolderName = "output"
    val outputFolder = new File(outputFolderName).getAbsoluteFile
    if (!outputFolder.exists()) outputFolder.mkdir()
    val outputFile = new File(s"$outputFolderName/queueing.txt").getAbsoluteFile
    if (outputFile.exists()) outputFile.delete()
    if (outputFile.createNewFile()) println("File created")
    val fileWriter = new FileWriter(outputFile)
    val bufferedWriter = new BufferedWriter(fileWriter)

    results.foreach(result => bufferedWriter.write(result.respData))

    bufferedWriter.flush()
    bufferedWriter.close()
    fileWriter.close()
  }
}

