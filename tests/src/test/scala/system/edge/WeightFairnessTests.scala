package system.edge

import akka.actor.ActorSystem
import akka.pattern.ask
import akka.util.Timeout
import common._
import common.rest.WskRestOperations
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.core.entity.CpuTime
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._


@RunWith(classOf[JUnitRunner])
class WeightFairnessTests extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll {
  val wskOperations: WskRestOperations = new WskRestOperations
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  val user1 = getAdditionalTestSubject("user1")
  val user2 = getAdditionalTestSubject("user2")
  val user3 = getAdditionalTestSubject("user3")
  val name = "mnv3"

  override def afterAll() = {
    disposeAdditionalTestSubject(user1.namespace)
    disposeAdditionalTestSubject(user2.namespace)
    disposeAdditionalTestSubject(user3.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user1.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "16"))

  wskadmin.cli(
    Seq("limits", "set", user2.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))

  wskadmin.cli(
    Seq("limits", "set", user3.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))

  it should "create three functions" in {
    wskOperations.action.create(
      name,
      None,
      docker = Some("binw/mnv3-action"),
      memory = Some(512.MB),
      cpu = Some(CpuTime(1000)))(user1)

    wskOperations.action.create(
      name,
      None,
      docker = Some("binw/mnv3-action"),
      memory = Some(512.MB),
      cpu = Some(CpuTime(1000)))(user2)


    wskOperations.action.create(
      name,
      None,
      docker = Some("binw/mnv3-action"),
      memory = Some(512.MB),
      cpu = Some(CpuTime(1000)))(user3)


    def staticLoad(timeSinceStart: FiniteDuration): Double = {
      1
    }

    def incrementing(timeSinceStart: FiniteDuration): Double = {
      if (timeSinceStart.toMinutes < 6) 0.1
      else 1
    }

    val system = ActorSystem("LoadGen")
    val user1Gen = system.actorOf(LoadGenerator.props(wskOperations, user1, name, 3.minutes, 9.minutes, incrementing))
    val user2Gen = system.actorOf(LoadGenerator.props(wskOperations, user2, name, 1.seconds, 9.minutes, staticLoad))
    val user3Gen = system.actorOf(LoadGenerator.props(wskOperations, user3, name, 1.seconds, 9.minutes, staticLoad))

    implicit val timeout = Timeout(30.minutes)
    val f1 = (user1Gen ? LoadGenerator.Start).mapTo[LoadGenerator.Message]
    val f2 = (user2Gen ? LoadGenerator.Start).mapTo[LoadGenerator.Message]
    val f3 = (user3Gen ? LoadGenerator.Start).mapTo[LoadGenerator.Message]

    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)
    Await.result(f3, Duration.Inf)

    system.terminate()
  }
}
