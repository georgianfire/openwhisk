package org.apache.openwhisk.core.loadBalancer

import java.time.Instant
import java.util.concurrent.ConcurrentLinkedQueue

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.event.Logging
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector.{ActivationMessage, ContainerOperationMessage, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.spi.SpiLoader

import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

/**
 * ## Limitations:
 * - We assume a single controller (loadbalancer) rather than a coordinated cluster
 * - We currently ignores managedFraction and blackboxFraction, i.e., we don't distinguish between managed functions and
 *   blackbox functions
 *
 * @param config
 * @param controllerInstance
 * @param feedFactory
 * @param invokerPoolFactory
 * @param messagingProvider
 * @param actorSystem
 * @param logging
 * @param materializer
 */
class EdgeBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  logging.info(this, s"Initializing $getClass")

  override protected def emitMetrics(): Unit = {
    super.emitMetrics()
    // TODO: Add other metrics
  }

  private implicit val random: Random = new Random()

  private val monitor = actorSystem.actorOf(Props(new Actor{
    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) => EdgeBalancerStateSingleton.updateInvokers(newState)

      // case CurrentContainerState(newState) => schedulingState.updateContainers(newState)
    }
  }))

  case class ActivationRecord(user: Namespace, action: FullyQualifiedEntityName, time: Instant)
  private val activationStore: ConcurrentLinkedQueue[ActivationRecord] = new ConcurrentLinkedQueue[ActivationRecord]

  sealed trait ResourceUpdate
  object Stay extends ResourceUpdate
  case class Add(sizes: IndexedSeq[CpuTime]) extends ResourceUpdate
  case class Remove(containers: IndexedSeq[ContainerInfo]) extends ResourceUpdate

  private val updateResources: Runnable = new Runnable {
    private val windowPeriod = 2.minutes
    private val burstWindowPeriod = 10.seconds

    override def run(): Unit = {
      logging.info(this, "Updating resources")

      val activations = activationStore.toArray(Array.empty[ActivationRecord])
      val now = Instant.now()
      val activationsByWindowStatus =
        activations.groupBy(record => record.time isAfter now.minusSeconds(windowPeriod.toSeconds))

      // Remove activations records that are outdated
      activationsByWindowStatus.getOrElse(false, Array.empty).foreach(activationStore.remove)

      val activationTimeByUserAction: Map[(Namespace, FullyQualifiedEntityName), Array[Instant]] =
        activationsByWindowStatus.getOrElse(true, Array.empty)
          .groupBy{ case ActivationRecord(user, action, _) => (user, action)}
          .mapValues(_.map(_.time))

      val predictedCapacityByUserAction: Map[(Namespace, FullyQualifiedEntityName), Seq[Int]] = activationTimeByUserAction.map { record =>
        val ((user, action), activationTimeArray) = record

        val arrivalRate: Double = {
          val longWindowArrivalRate = activationTimeArray.length.doubleValue / windowPeriod.toSeconds
          val burstWindowArrivalRate = activationTimeArray.count(_.isAfter(now.minusSeconds(burstWindowPeriod.toSeconds))).doubleValue / burstWindowPeriod.toSeconds
          if (burstWindowArrivalRate >= 2 * longWindowArrivalRate) burstWindowArrivalRate
          else longWindowArrivalRate
        }

        logging.info(this, s"$action has arrival rate $arrivalRate")

        val containers = EdgeBalancerStateSingleton.getContainers(user, action).getOrElse(IndexedSeq.empty)
        val standardCpu = EdgeBalancerStateSingleton.actionStandardCpuMap(action)
        val maxInstances = 100 // Should be read from action limits

        if (containers.isEmpty || containers.forall(_.cpu == standardCpu)) {
          // All current containers have the standard size (Homogeneous)
          val serviceRate = getServiceRate(action, standardCpu)
          val predictedCapacity = planHomogeneous(arrivalRate, serviceRate, 100.milliseconds, maxInstances)
          ((user, action), List.fill(predictedCapacity)(standardCpu.milliCpus))
        } else {
          // Heterogeneous
          val currentCapacity =
            EdgeBalancerStateSingleton.getContainers(user, action)
            .getOrElse(IndexedSeq.empty)
            .map(_.cpu)

          val currentServiceRates = currentCapacity.map(cpu => getServiceRate(action, cpu))
          val standardServiceRate = getServiceRate(action, standardCpu)
          val predictedServiceRates =
            planHeterogeneous(arrivalRate, currentServiceRates, standardServiceRate, 100.milliseconds, maxInstances)
          val predictedCapacity = if (predictedServiceRates.length >= currentServiceRates.length) {
            val n = predictedServiceRates.length - currentServiceRates.length
            currentCapacity.map(_.milliCpus) ++ List.fill(n)(standardCpu.milliCpus)
          } else {
            val n = currentServiceRates.length - predictedServiceRates.length
            currentCapacity.map(_.milliCpus).sorted.drop(n)
          }
          ((user, action), predictedCapacity)
        }
      }

      val allUserAction = (EdgeBalancerStateSingleton.getScheduledUserActions ++ predictedCapacityByUserAction.keys).distinct

      val operationByUserAction: Map[(Namespace, FullyQualifiedEntityName), ResourceUpdate] = allUserAction.map(userAction => {
        val (user, action) = userAction
        val currentContainers = EdgeBalancerStateSingleton.getContainers(user, action).getOrElse(Seq.empty)
        val predictedContainerSizes = predictedCapacityByUserAction.get(user, action).getOrElse(Seq.empty)
        val updateOperation = if (currentContainers.length == predictedContainerSizes.length) {
          Stay
        } else if (currentContainers.length < predictedContainerSizes.length) {
          val n = predictedContainerSizes.length - currentContainers.length
          val standardCpu = EdgeBalancerStateSingleton.actionStandardCpuMap(action)
          Add(IndexedSeq.fill(n)(standardCpu))
        } else {
          val n = currentContainers.length - predictedContainerSizes.length
          Remove(currentContainers.sortBy(_.cpu).take(n).toIndexedSeq)
        }

        ((user, action), updateOperation)
      }).toMap

      val containerToBeCreated = operationByUserAction.collect{
        case ((user, action), Add(sizes)) => sizes.map(size => ContainerToBeCreated(user, action, size))
      }.flatten.toIndexedSeq

      tryPacking(containerToBeCreated) match {
        case Some(solution) => solution.foreach{ record =>
          val (ContainerToBeCreated(user, action, cpu), invoker) = record
          createContainer(user, action, cpu, invoker)
        }
        case None =>
          // Resource contention is happening, first do a garbage collection
          operationByUserAction.foreach {
            case ((user, action), Remove(containers)) => containers.foreach{ container =>
              val containerRemovalMessage = ContainerOperationMessage(ContainerOperationMessage.ForceTerminate, container.id)
              sendControlMessageToInvoker(messageProducer, containerRemovalMessage, container.invoker).map(_ =>
                deleteContainer(user, action, container.id)
              )
            }
            case _ => // do nothing
          }

          tryPacking(containerToBeCreated) match {
            case Some(solution) =>
              solution.foreach{ case (ContainerToBeCreated(user, action, cpu), invoker) =>
                createContainer(user ,action, cpu, invoker)
              }
            case None =>
              val globalWeightByFunction = calculateGlobalWeight(allUserAction)
              val quotaPerFunction = calculateQuota(allUserAction, globalWeightByFunction, predictedCapacityByUserAction)

              execute(allUserAction, quotaPerFunction, predictedCapacityByUserAction, operationByUserAction)
          }

      }

      logging.info(this, s"$predictedCapacityByUserAction")
    }

    def planHomogeneous(arrivalRate: Double,
                        serviceRate: Double,
                        p95WaitingTime: FiniteDuration,
                        maxInstances: Int): Int = {
      def factorial(n: Int): BigInt = {
        @tailrec
        def loop(acc: BigInt, i: Int): BigInt = {
          if (i ==0) acc
          else loop(acc * i, i - 1)
        }

        loop(1, n)
      }

      logging.info(this, s"calculating queueing model for function with arrival rate $arrivalRate and service rate $serviceRate")

      val r = arrivalRate / serviceRate
      (1 to maxInstances).find { c =>
        import math.{pow, floor}

        val rho = r / c
        if (rho >= 1) {
          false
        } else {
          val p_0 = {
            val temp = pow(r, c) / (factorial(c).doubleValue * (1 - rho)) +
              (0 until c).map(n => pow(r, n) / factorial(n).doubleValue).sum
            pow(temp, -1)
          }

          val l = floor((p95WaitingTime.toMillis.doubleValue / 1000) * c * serviceRate + c - 1).toInt
          val p = (0 to l).map { n=>
            if (n < c) {
              p_0 * pow(r, n) / factorial(n).doubleValue
            } else {
              p_0 * pow(r, n) / (pow(c, n-c) * factorial(c).doubleValue)
            }
          }.sum

          require(!p.isNaN)
          p >= 0.95
        }
      }.getOrElse(maxInstances)
    }

    def planHeterogeneous(arrivalRate: Double,
                          currentServiceRates: IndexedSeq[Double],
                          standardServiceRate: Double,
                          p95WaitingTime: FiniteDuration,
                          maxInstances: Int): IndexedSeq[Double] = {

      def isEnough(serviceRates: IndexedSeq[Double]): Boolean = {
        import math.{pow, floor}

        val c = serviceRates.length
        val sortedServiceRates = serviceRates.sorted

        def m(i: Int): Double = {
          if (i >= c) {
            sortedServiceRates.sum
          } else {
            sortedServiceRates.take(i).sum
          }
        }

        def mproduct(i: Int): Double = {
          (1 to i).map(m).foldLeft(1.0)(_ * _)
        }

        if (c == 0 || arrivalRate / m(c) >= 1) {
          false
        } else {
          val rho =arrivalRate / m(c)
          val p_0 = {
            val temp = (0 until c).map(i =>
              pow(arrivalRate, i) / mproduct(i)
              ).sum + pow(arrivalRate, c) / ((1 - rho) * mproduct(c))
            pow(temp, -1)
          }

          val l = floor((p95WaitingTime.toMillis / 1000) * m(c) + c - 1).toInt
          val p = (0 to l).map(i =>
            if (i < c) {
              p_0 * pow(arrivalRate, i) / mproduct(i)
            } else {
              p_0 * (1 to c).map(j => arrivalRate / m(j)).foldLeft(1.0)(_ * _) *
                ((c + 1) to i).map(j => arrivalRate / m(c)).foldLeft(1.0)(_ * _)
            }
          ).sum
          p >= 0.95
        }
      }

      val sortedServiceRates = currentServiceRates.sorted
      val currentInstanceNum = sortedServiceRates.length
      if (isEnough(sortedServiceRates)) {
        val n = (1 to currentInstanceNum).find{ n =>
          !isEnough(sortedServiceRates.drop(n))
        }.get
        sortedServiceRates.drop(n - 1)
      } else {
        val n = (1 to (maxInstances - currentInstanceNum)).find( n =>
          isEnough(sortedServiceRates ++ List.fill(n)(standardServiceRate))
        ).getOrElse(maxInstances - currentInstanceNum)
        sortedServiceRates ++ List.fill(n)(standardServiceRate)
      }

    }

    case class ContainerToBeCreated(user: Namespace, action: FullyQualifiedEntityName, cpu: CpuTime)
    def tryPacking(containersToBeCreated: IndexedSeq[ContainerToBeCreated]): Option[Map[ContainerToBeCreated, InvokerInstanceId]] = {
      var capacityByInvoker = EdgeBalancerStateSingleton.getAvailableSpaces
      val solution: collection.mutable.Map[ContainerToBeCreated, InvokerInstanceId] =
        collection.mutable.Map.empty
      for (container <- containersToBeCreated.sortBy(_.cpu).reverse) {
        val (invoker, capacity) = capacityByInvoker.maxBy(_._2)
        if (capacity >= container.cpu) {
          solution += (container -> invoker)
          capacityByInvoker = capacityByInvoker.updated(invoker, capacity - container.cpu)
        } else {
          return None
        }
      }
      Some(solution.toMap)
    }

    def calculateGlobalWeight(allUserAction: List[(Namespace, FullyQualifiedEntityName)]): Map[(Namespace, FullyQualifiedEntityName), Double] = {
      val totalActionWeightUnderUser = allUserAction.map{ case (user, action) =>
        (user, EdgeBalancerStateSingleton.actionWeight(action))
      }.groupBy(_._1).map { case (user, actionWeights) =>
        (user, actionWeights.map(_._2).sum)
      }

      val totalUserWeight =
        allUserAction
          .map(_._1)
          .distinct
          .map(user => EdgeBalancerStateSingleton.userWeight(user))
          .sum

      allUserAction.map{ case (user, action) =>
        (user, action) -> (EdgeBalancerStateSingleton.actionWeight(action).doubleValue / totalActionWeightUnderUser(user)) *
          (EdgeBalancerStateSingleton.userWeight(user).doubleValue / totalUserWeight)
      }.toMap
    }

    def calculateQuota(allUserAction: List[(Namespace, FullyQualifiedEntityName)],
                       globalWeight: Map[(Namespace, FullyQualifiedEntityName), Double],
                       predictedCapacityByUserAction: Map[(Namespace, FullyQualifiedEntityName), Seq[Int]]): Map[(Namespace, FullyQualifiedEntityName), Int] = {
      val totalCapacityOfAllInvokers = EdgeBalancerStateSingleton.invokers.map(_.id.userCpu.milliCpus).sum
      val functionsSortedByWeightedCapacity =  allUserAction.sortBy( function =>
        predictedCapacityByUserAction(function).sum / globalWeight(function)
      )

      @tailrec
      def loop(capacity: Double,
               remainingFunctionsSortedByWeightedCapacity: List[(Namespace, FullyQualifiedEntityName)],
               quotaPerFunction: Map[(Namespace, FullyQualifiedEntityName), Int]): Map[(Namespace, FullyQualifiedEntityName), Int] = {
        import math.floor
        if (remainingFunctionsSortedByWeightedCapacity.isEmpty) quotaPerFunction
        else {
          val function = remainingFunctionsSortedByWeightedCapacity.head
          val quota = math.min(predictedCapacityByUserAction(function).sum,
                               floor(capacity * globalWeight(function) / remainingFunctionsSortedByWeightedCapacity.map(function => globalWeight(function)).sum).intValue)

          loop(capacity - quota, remainingFunctionsSortedByWeightedCapacity.tail, quotaPerFunction.updated(function, quota))
        }
      }

      loop(totalCapacityOfAllInvokers, functionsSortedByWeightedCapacity, Map.empty)
    }

    def execute(allUserAction: List[(Namespace, FullyQualifiedEntityName)],
                quotaPerFunction: Map[(Namespace, FullyQualifiedEntityName), Int],
                predictedCapacityByUserAction: Map[(Namespace, FullyQualifiedEntityName), Seq[Int]],
                operationByUserAction: Map[(Namespace, FullyQualifiedEntityName), ResourceUpdate]): Unit = {

      var addAfterwards: Map[(Namespace, FullyQualifiedEntityName), Int] = Map.empty

      allUserAction.foreach { case function @ (user, action) =>
        val currentContainers = EdgeBalancerStateSingleton.getContainers(user, action).getOrElse(IndexedSeq.empty)

        val quota = quotaPerFunction(function)

        operationByUserAction(function) match {
          case Add(cpuTimes) =>
            val currentCapacity = currentContainers.map(_.cpu.milliCpus).sum
            if (quota > currentCapacity) {
              addAfterwards = addAfterwards + (function -> math.min(cpuTimes.map(_.milliCpus).sum, quota - currentCapacity))
            } else if (quota < currentCapacity) {
              terminateOrDeflate(function, currentContainers, quota)
            }

          case Remove(removedContainers) =>
            val predictedCapacity = predictedCapacityByUserAction.getOrElse(function, Seq.empty).sum
            if (predictedCapacity > quota) {
              terminateOrDeflate(function, currentContainers.diff(removedContainers), quota)
            }

          case Stay =>
            val predictedCapacity = predictedCapacityByUserAction.getOrElse(function, Seq.empty).sum
            if (predictedCapacity > quota) {
              terminateOrDeflate(function, currentContainers, quota)
            }
        }
      }

      while (addAfterwards.nonEmpty) {
        val (function, remainingQuota) = addAfterwards.maxBy(_._2)
        val (user, action) = function
        val standarcCpu = EdgeBalancerStateSingleton.actionStandardCpuMap(action).milliCpus
        val minCpu = standarcCpu / 2
        if (remainingQuota < minCpu) {
          addAfterwards = addAfterwards - function
        } else {
          val invokerCapacity = EdgeBalancerStateSingleton.getAvailableSpaces
          val maxInvokerCapacity = invokerCapacity.values.max
          if (maxInvokerCapacity.milliCpus < minCpu) {
            addAfterwards = addAfterwards - function
          } else {
            val cpu = CpuTime(maxInvokerCapacity.milliCpus min remainingQuota min standarcCpu)
            val canditates = invokerCapacity.filter(_._2 >= cpu).keys.toList
            val invoker = canditates(random.nextInt(canditates.length))
            createContainer(user ,action, cpu, invoker)
            addAfterwards = addAfterwards.updated(function, remainingQuota - cpu.milliCpus)
          }
        }
      }
    }

    def terminateOrDeflate(function: (Namespace, FullyQualifiedEntityName),
                           containers: Seq[ContainerInfo],
                           quota: Int,
                           useDeflation: Boolean = false) = Future {
      val (user, action) = function
      if (useDeflation) {
        val standardCpu = EdgeBalancerStateSingleton.actionStandardCpuMap(action)
        val minCpu = standardCpu.milliCpus / 2
        if (quota < minCpu * containers.length) {
          // some containers need to be terminated
          val num = quota / minCpu

          val sortedContainers = containers.sortBy(_.cpu)

          sortedContainers.dropRight(num).foreach { container =>
            val containerRemovalMessage = ContainerOperationMessage(
              operation = ContainerOperationMessage.ForceTerminate,
              containerId = container.id
            )
            sendControlMessageToInvoker(messageProducer, containerRemovalMessage, container.invoker).map( _ =>
              deleteContainer(user, action, container.id)
            )
          }

          sortedContainers.takeRight(num).foreach { container =>
            val newCpu = CpuTime(quota / num)
            val containerResizeMessage = ContainerOperationMessage (
              operation = ContainerOperationMessage.Resize,
              containerId = container.id,
              cpu = Some(newCpu)
            )
            sendControlMessageToInvoker(messageProducer, containerResizeMessage, container.invoker).map( _ =>
              resizeContainer(user, action, container.id, newCpu)
            )
          }
        } else {
          val usedCapacity = containers.map(_.cpu.milliCpus).sum
          val deflateObj = usedCapacity - quota
          val deflatable = usedCapacity - containers.length * minCpu
          containers.foreach { container =>
            val newCpu = CpuTime(container.cpu.milliCpus - math.ceil(deflateObj * (container.cpu.milliCpus - minCpu).doubleValue / deflatable).intValue)
            val containerResizeMesage = ContainerOperationMessage(
              operation = ContainerOperationMessage.Resize,
              containerId = container.id,
              cpu = Some(newCpu)
            )
            sendControlMessageToInvoker(messageProducer, containerResizeMesage, container.invoker).map( _ =>
              resizeContainer(user, action, container.id, newCpu)
            )
          }
        }
      } else {
        var totalReleased = 0
        val obj = containers.map(_.cpu.milliCpus).sum - quota
        random.shuffle(containers).takeWhile { container =>
          totalReleased += container.cpu.milliCpus
          totalReleased < obj
        }.foreach { container =>
          val containerRemovalMessage = ContainerOperationMessage(
            operation = ContainerOperationMessage.ForceTerminate,
            containerId = container.id
          )
          sendControlMessageToInvoker(messageProducer, containerRemovalMessage, container.invoker).map( _ =>
            deleteContainer(user, action, container.id)
          )
        }
      }
    }

  }

  class ServiceRatePredictor(profilingResults: Map[Int, Double]) {
    val minCpu = profilingResults.keys.min
    val maxCpu = profilingResults.keys.max

    def predict(cpu: Int): Double = {
      require(cpu >= minCpu && cpu <= maxCpu, "cpu is out of range")
      val serviceTime = if (profilingResults contains cpu) {
        profilingResults(cpu)
      } else {
        // try to do Interpolation
        val (lowerKey, lowerValue) = profilingResults.filterKeys(_ < cpu).max
        val (upperKey, upperValue) = profilingResults.filterKeys(_ > cpu).min
        lowerValue + (upperValue - lowerValue) * (cpu - lowerKey) / (upperKey - lowerKey)
      }
      1000 / serviceTime
    }
  }

  private val mnv3Predictor = new ServiceRatePredictor(Map(
    500 -> 1536.702,
    600 -> 1409.370,
    700 -> 1219.612,
    800 -> 892.214,
    900 -> 758.574,
    1000 -> 691.208))

  private val primePredictor = new ServiceRatePredictor(Map(
    200 -> 259.760,
    300 -> 177.540,
    400 -> 117.242
  ))

  def getServiceRate(action: FullyQualifiedEntityName, containerCpu: CpuTime): Double = {
    action.name.asString match {
      // the mobilenet V3 action
      case name if name.startsWith("mnv3") =>
        mnv3Predictor.predict(containerCpu.milliCpus)
      case name if name.startsWith("prime") =>
        primePredictor.predict(containerCpu.milliCpus)
      case name =>
        throw new NotImplementedError(s"Function $name has not been profiled")
    }
  }

  private val schedulingPeriod: FiniteDuration = 5.seconds
  actorSystem.scheduler.schedule(schedulingPeriod, schedulingPeriod, updateResources)

  private def createContainer(user: Namespace, action: FullyQualifiedEntityName, cpu: CpuTime, invoker: InvokerInstanceId) = {
    val containerId = UUID().asString
    val memory = EdgeBalancerStateSingleton.actionMemoryMap(action)
    val containerCreationMessage = ContainerOperationMessage(
      ContainerOperationMessage.Create,
      containerId,
      Some(memory),
      Some(cpu),
      Some(action)
    )
    sendControlMessageToInvoker(messageProducer, containerCreationMessage, invoker).map(_ =>
      addContainer(user, action, ContainerInfo(containerId, invoker, memory, cpu))
    )
  }

  override val invokerPool: ActorRef = invokerPoolFactory.createInvokerPool(
    actorSystem,
    messagingProvider,
    messageProducer,
    sendActivationToInvoker,
    Some(monitor)
  )

  override def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {
    // nothing needs to be done here
  }

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(EdgeBalancerStateSingleton.invokers)

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)
                      (implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    logging.info(this, s"User: ${msg.user.namespace.name.asString} with weight ${msg.user.limits.weight}")
    logging.info(this, s"Action: ${action.name.asString} with weight: ${action.limits.weight}")

    // Update state
    if (!EdgeBalancerStateSingleton.userWeight.contains(msg.user.namespace)) {
      val weight = msg.user.limits.weight.getOrElse(Weight()).weightValue
      EdgeBalancerStateSingleton.userWeight = EdgeBalancerStateSingleton.userWeight.updated(msg.user.namespace, weight)
    }

    if (!EdgeBalancerStateSingleton.actionWeight.contains(msg.action)) {
      EdgeBalancerStateSingleton.actionWeight =
        EdgeBalancerStateSingleton.actionWeight.updated(msg.action, action.limits.weight.weightValue)
      EdgeBalancerStateSingleton.actionStandardCpuMap =
        EdgeBalancerStateSingleton.actionStandardCpuMap.updated(msg.action, action.limits.cpu.cpuTime)
      EdgeBalancerStateSingleton.actionMemoryMap =
        EdgeBalancerStateSingleton.actionMemoryMap.updated(msg.action, action.limits.memory.megabytes.MB)
    }

    activationStore.add(ActivationRecord(msg.user.namespace, msg.action, Instant.now))

    // Attempt to schedule
    EdgeBalancer.schedule(msg.user.namespace, msg.action, EdgeBalancerStateSingleton).map{ containerInfo =>
      val invoker = containerInfo.invoker
      val msgWithContainerId = msg.copy(containerId = Some(containerInfo.id), size=Some((containerInfo.memory, containerInfo.cpu)))
      val activationResult = setupActivation(msgWithContainerId, action, invoker)
      sendActivationToInvoker(messageProducer, msgWithContainerId, invoker).map(_ => activationResult)
    }.getOrElse {
      // This should be due to the action has no running containers.
      // Create one container and schedule to it.

      // randomly pick a invoker with the highest available capacity
      val (invoker, availableCpu) = {
        val sorted = EdgeBalancerStateSingleton.getAvailableSpaces.toList.sortBy(_._2).reverse
        val candidates = sorted.takeWhile(_._2 == sorted.head._2)
        candidates(random.nextInt(candidates.length))
      }

      if (availableCpu >= action.limits.cpu.cpuTime) {
        // If the invoker has enough space then schedule
        val containerId = UUID().asString
        val memory = action.limits.memory.megabytes.MB
        val cpu = action.limits.cpu.cpuTime

        val coldStartMsg = msg.copy(containerId = Some(containerId), size = Some((memory, cpu)))
        val activationResult = setupActivation(coldStartMsg, action, invoker)
        sendActivationToInvoker(messageProducer, coldStartMsg, invoker) map { _ =>
          addContainer(msg.user.namespace, msg.action, ContainerInfo(containerId, invoker, memory, cpu))
          activationResult
        }
      } else {
        // Give up for now
        Future.failed(new NotImplementedError())
      }
    }
  }

  def addContainer(user: Namespace, action: FullyQualifiedEntityName, containerInfo: ContainerInfo): Future[Unit] = {
    Future {
      var updated = false
      while (!updated) {
        EdgeBalancerStateSingleton.getContainers(user, action) match {
          case Some(oldContainers) =>
            updated = EdgeBalancerStateSingleton.compareAndSetContainers(user, action, oldContainers, oldContainers :+ containerInfo)
          case None =>
            updated = EdgeBalancerStateSingleton.putIfAbsent(user, action, IndexedSeq(containerInfo))
        }
      }
      logging.info(this, s"Added container with ${containerInfo.memory} memory and ${containerInfo.cpu} cpu for action $user/$action")
    }
  }

  def deleteContainer(user: Namespace, action: FullyQualifiedEntityName, containerId: String): Future[Unit] = {
    Future {
      var updated = false
      while (!updated) {
        EdgeBalancerStateSingleton.getContainers(user, action) match {
          case Some(oldContainers) =>
            updated = if (oldContainers.exists(_.id == containerId)) {
              EdgeBalancerStateSingleton.compareAndSetContainers(user, action, oldContainers, oldContainers.filter(_.id != containerId))
            } else {
              true // already deleted
            }
          case None =>
            logging.warn(this, s"Cannot find containers for $user/$action, this really should not happen")
            updated = true
        }
      }
      logging.info(this, s"Removed container for action $user/$action")
    }
  }

  def resizeContainer(user: Namespace, action: FullyQualifiedEntityName, containerId: String, cpu: CpuTime): Future[Unit] = {
    Future {
      var updated = false
      while (!updated) {
        val oldContainers = EdgeBalancerStateSingleton.getContainers(user, action).get
        val updatedCntainers = oldContainers.map { containerInfo =>
          if (containerInfo.id == containerId) containerInfo.copy(cpu = cpu)
          else containerInfo
        }
        updated = EdgeBalancerStateSingleton.compareAndSetContainers(user, action, oldContainers, updatedCntainers)
      }
      logging.info(this, s"Resized container $containerId for action $user/$action to $cpu")
    }
  }

  private def sendControlMessageToInvoker(producer: MessageProducer, msg: ContainerOperationMessage,  invoker: InvokerInstanceId) = {
    implicit val transactionId = msg.transid

    val topic = s"invoker${invoker.toInt}-control"

    val start = transactionId.started(
      this,
      LoggingMarkers.CONTROLLER_KAFKA,
      s"posting topic '$topic' with $msg'",
      logLevel = Logging.InfoLevel)

    producer.send(topic, msg).andThen {
      case Success(status) =>
        transactionId.finished(
          this,
          start,
          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
          logLevel = Logging.InfoLevel)
      case Failure(_) => transactionId.failed(this, start, s"error on posting to topic $topic")
    }
  }
}

object EdgeBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = kafkaHosts

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {
    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messageProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messageProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor
          )
        )
      }
    }

    new EdgeBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def schedule(user: Namespace,
               action: FullyQualifiedEntityName,
               edgeBalancerState: EdgeBalancerState)(implicit random: Random): Option[ContainerInfo] = {
    edgeBalancerState.getContainers(user, action).flatMap { containers =>
      val weights = containers.map(_.weight)
      weightedRandomChoice(containers.zip(weights))
    }
  }

  private def weightedRandomChoice[T](weightedItems: IndexedSeq[(T, Int)])(implicit random: Random): Option[T] = {
    if (weightedItems.nonEmpty) {
      val weights = weightedItems.map(_._2)
      require(weights.forall(_ >= 0), new IllegalArgumentException("Weights must be non-negative"))

      val weightSum = weights.sum
      if (weightSum != 0) {
        val randomValue = random.nextInt(weightSum)
        val choiceIndex = weights.scanLeft(0)(_ + _).sliding(2).zipWithIndex.collectFirst {
          case Tuple2(IndexedSeq(start, end), index) if start until end contains randomValue => index
        }

        assert(choiceIndex.isDefined)

        choiceIndex.map(index => weightedItems(index)._1)
      } else {
        // Non-empty items but all the weights are 0
        None
      }
    } else {
      // Empty items
      None
    }
  }
}

trait EdgeBalancerState {
  def invokers: IndexedSeq[InvokerHealth]

  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit

  def getContainers(user: Namespace, action: FullyQualifiedEntityName): Option[IndexedSeq[ContainerInfo]]

  def compareAndSetContainers(user: Namespace,
                              action: FullyQualifiedEntityName,
                              expected: IndexedSeq[ContainerInfo],
                              newContainers: IndexedSeq[ContainerInfo]): Boolean

  def putIfAbsent(user: Namespace,
                  action: FullyQualifiedEntityName,
                  newContainers: IndexedSeq[ContainerInfo]): Boolean

  def getScheduledUserActions: List[(Namespace, FullyQualifiedEntityName)]
}

object EdgeBalancerStateSingleton extends EdgeBalancerState {
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty

  private val _containersPerAction: collection.concurrent.Map[(Namespace, FullyQualifiedEntityName), IndexedSeq[ContainerInfo]] =
    collection.concurrent.TrieMap.empty

  override def invokers: IndexedSeq[InvokerHealth] = _invokers

  override def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = this._invokers = newInvokers

  override def getContainers(user: Namespace, action: FullyQualifiedEntityName): Option[IndexedSeq[ContainerInfo]] =
    this._containersPerAction get (user, action)

  override def compareAndSetContainers(user: Namespace,
                                       action: FullyQualifiedEntityName,
                                       expected: IndexedSeq[ContainerInfo],
                                       newContainers: IndexedSeq[ContainerInfo]): Boolean = {
    this._containersPerAction.replace((user, action), expected, newContainers)
  }

  override def putIfAbsent(user: Namespace,
                           action: FullyQualifiedEntityName,
                           newContainers: IndexedSeq[ContainerInfo]): Boolean =
    this._containersPerAction.putIfAbsent((user, action), newContainers).isEmpty

  override def getScheduledUserActions: List[(Namespace, FullyQualifiedEntityName)] = this._containersPerAction.keys.toList

  var userWeight = Map.empty[Namespace, Int]
  var actionWeight = Map.empty[FullyQualifiedEntityName, Int]
  var actionStandardCpuMap = Map.empty[FullyQualifiedEntityName, CpuTime]
  var actionMemoryMap = Map.empty[FullyQualifiedEntityName, ByteSize]

  def getAvailableSpaces: Map[InvokerInstanceId, CpuTime] = {
    val usedMilliCpusByInvoker: Map[InvokerInstanceId, Int] = _containersPerAction.values.flatten
      .groupBy(containerInfo => containerInfo.invoker)
      .map{ record =>
        val (invoker, containers) = record
        (invoker, containers.map(_.cpu.milliCpus).sum)
      }

    invokers.map { invokerHealth =>
      val instance = invokerHealth.id
      val availabieCpu = {
        val availableMilliCpus= instance.userCpu.milliCpus - usedMilliCpusByInvoker.getOrElse(instance, 0)
        if (availableMilliCpus >= 0) CpuTime(availableMilliCpus)
        else CpuTime(0)
      }
      (instance, availabieCpu)
    }.toMap
  }
}

case class ContainerInfo(
  id: String,
  invoker: InvokerInstanceId,
  memory: ByteSize,
  cpu: CpuTime) {
  def weight: Int = cpu.milliCpus
}
