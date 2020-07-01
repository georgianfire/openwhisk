package system.edge

import java.time.Instant

import akka.actor.{Actor, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import common.rest.{RestResult, WskRestOperations}
import common._
import org.apache.openwhisk.core.containerpool.Interval
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import org.apache.openwhisk.core.entity.CpuTime
import org.apache.openwhisk.core.entity.size._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

class LoadGenerator(wskOperations: WskRestOperations,
                    user: WskProps,
                    action: String,
                    startAfter: FiniteDuration,
                    stopAfter: FiniteDuration,
                    arrivalRateGenerator: FiniteDuration => Double) extends Actor {
  implicit val random = new Random()

  override def receive: Receive = {
    case LoadGenerator.Start =>
      try {
        val start = Instant.now
        implicit val ec = context.dispatcher

        Thread.sleep(startAfter.toMillis)

        var futures: Seq[Future[RestResult]] = Seq.empty
        while (Interval(start, Instant.now).duration < stopAfter) {
          futures = futures :+ Future {
            wskOperations.action.invoke(action)(user)
          }

          val arrivalRate = arrivalRateGenerator(Interval(start, Instant.now).duration)
          Thread.sleep((LoadGenerator.genExponential(arrivalRate) * 1000).intValue)
        }

        Await.result(Future.sequence(futures), Duration.Inf)

        sender() ! LoadGenerator.Finished
      } catch {
        case e: Throwable =>
          sender() ! akka.actor.Status.Failure(e)
          throw e
      }
  }
}

object LoadGenerator {
  sealed trait Message
  object Start extends Message
  object Finished extends Message

  def props(wskOperations: WskRestOperations,
            user: WskProps,
            action: String,
            startAfter: FiniteDuration,
            stopAfter: FiniteDuration,
            arrivalRateGenerator: FiniteDuration => Double) = Props(new LoadGenerator(wskOperations, user, action, startAfter, stopAfter, arrivalRateGenerator))

  def genExponential(lambda: Double)(implicit rng: Random): Double = {
    math.log(1 - rng.nextDouble()) / (-lambda)
  }
}

@RunWith(classOf[JUnitRunner])
class FairnessTests  extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll {
  val wskOperations: WskRestOperations = new WskRestOperations
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  val user1 = getAdditionalTestSubject("user1")
  val user2 = getAdditionalTestSubject("user2")


  override def afterAll() = {
    disposeAdditionalTestSubject(user1.namespace)
    disposeAdditionalTestSubject(user2.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user1.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))

  wskadmin.cli(
    Seq("limits", "set", user2.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))


  it should "create function 1" in {
    wskOperations.action.create(
      "prime",
      Some(TestUtils.getTestActionFilename("prime.py")),
      memory = Some(256.MB),
      cpu = Some(CpuTime(400)),
      expectedExitCode = TestUtils.DONTCARE_EXIT)(user1)

    wskOperations.action.create(
      "mnv3",
      None,
      docker = Some("binw/mnv3-action"),
      memory = Some(512.MB),
      cpu = Some(CpuTime(1000)),
      expectedExitCode = TestUtils.DONTCARE_EXIT)(user2)

    def arrivalRateGeneratorMnv3(timeSinceStart: FiniteDuration):Double = {
      3
    }
    def arrivalRateGeneratorPrime(timeSinceStart: FiniteDuration): Double = {
      if (timeSinceStart.toMinutes < 10 ) 25
      else if (timeSinceStart.toMinutes < 15) 30
      else 50
    }

    val system = ActorSystem("LoadGen")
    val primeGen = system.actorOf(LoadGenerator.props(wskOperations, user1, "prime", 1.seconds, 25.minutes, arrivalRateGeneratorPrime))
    val mnv3Gen = system.actorOf(LoadGenerator.props(wskOperations, user2, "mnv3", 5.minutes, 20.minutes, arrivalRateGeneratorMnv3))
    implicit val timeout = Timeout(30.minutes)

    val f1 = (primeGen ? LoadGenerator.Start).mapTo[LoadGenerator.Message]
    val f2 = (mnv3Gen ? LoadGenerator.Start).mapTo[LoadGenerator.Message]

    Await.result(f1, Duration.Inf)
    Await.result(f2, Duration.Inf)

    system.terminate()
  }
}
