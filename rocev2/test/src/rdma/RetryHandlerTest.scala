package rdma

import spinal.core._
import spinal.core.sim._

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.mutable

import ConstantSettings._
import RdmaConstants._
import StreamSimUtil._
import RdmaTypeReDef._
import PsnSim._
//import OpCodeSim._
import WorkReqSim._
import SimSettings._

class NormalAndRetryWorkReqHandlerTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U1024
  val maxFragNum = 37

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(SpinalConfig(anonymSignalPrefix = "tmp"))
    .compile(new NormalAndRetryWorkReqHandler(busWidth))

  type WorkReqData = (
      WorkReqId,
      SpinalEnumElement[WorkReqOpCode.type],
      VirtualAddr,
      LRKey,
      AckReq,
      AtomicSwap,
      AtomicComp,
      ImmData,
      VirtualAddr,
      PktLen,
      LRKey
  )
  type RetryWorkReqData = (
      WorkReqId,
      SpinalEnumElement[WorkReqOpCode.type],
      VirtualAddr,
      LRKey,
      AckReq,
      AtomicSwap,
      AtomicComp,
      ImmData,
      VirtualAddr,
      PktLen,
      LRKey,
      PsnStart,
      PktNum
  )

  test("NormalAndRetryWorkReqHandler normal case") {
    testFunc(normalReqOrRetryReq = true, retryExceeded = false)
  }

  test("NormalAndRetryWorkReqHandler retry case") {
    testFunc(normalReqOrRetryReq = false, retryExceeded = false)
  }

  def testFunc(normalReqOrRetryReq: Boolean, retryExceeded: Boolean): Unit =
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      QpCtrlSim.connectNormalAndRetryWorkReqHandler(
        dut.clockDomain,
        pmtuLen,
        dut.io.psnInc,
        dut.io.errNotifier,
        dut.io.retryClear,
        dut.io.retryScanCtrlBus,
        dut.io.qpAttr,
        dut.io.txQCtrl
      )

      val retryWorkReqGenQueue = mutable.Queue[RetryWorkReqData]()
      val inputWorkReqQueue = mutable.Queue[WorkReqData]()
      val inputRetryWorkReqQueue = mutable.Queue[RetryWorkReqData]()
      val retryOutputQueue =
        mutable.Queue[(PSN, OpCode.Value, PadCnt, MTY, FragLast)]()
      val expectedOutputQueue =
        mutable.Queue[(PSN, OpCode.Value, PadCnt, MTY, FragLast)]()
      val actualOutputQueue =
        mutable.Queue[(PSN, OpCode.Value, PadCnt, MTY, FragLast)]()

      // Input to DUT
      DmaReadBusSim(busWidth).reqStreamFixedDelayAndMultiRespSuccess(
        dut.io.dmaRead,
        dut.clockDomain,
        fixedRespDelayCycles = DMA_READ_DELAY_CYCLES
      )
//    val _ = SqDmaBusSim.reqStreamAlwaysFireAndRespSuccess(
//      dut.io.dma,
//      dut.clockDomain,
//      busWidth
//    )
      // addrCacheRespQueue
      val _ = AddrCacheSim.reqStreamAlwaysFireAndRespSuccess(
        dut.io.addrCacheRead,
        dut.clockDomain
      )

      fork {
        val numWorkReqGen = 10
        val workReqAckReq = true

        while (true) {
          dut.clockDomain.waitSampling()

          val workReqGen = WorkReqSim.randomWorkReqGen(
            dut.io.workReq,
            workReqAckReq,
            numWorkReqGen,
            maxFragNum,
            busWidth
          )
          inputWorkReqQueue.appendAll(workReqGen)

          println(
            f"${simTime()} time: inputWorkReqQueue.size=${inputWorkReqQueue.size} && expectedOutputQueue.size=${expectedOutputQueue.size}"
          )
          dut.clockDomain.waitSamplingWhere(
            inputWorkReqQueue.isEmpty && expectedOutputQueue.isEmpty
          )
          println(f"${simTime()} time: normal WR process done")

          if (!normalReqOrRetryReq) {
            inputRetryWorkReqQueue.appendAll(retryWorkReqGenQueue)
            expectedOutputQueue.appendAll(retryOutputQueue)

            // Before retry settings
            dut.io.sqHasPendingDmaReadReq #= true

            // Generate retry start pulse
            dut.io.txQCtrl.retryStartPulse #= true
            dut.clockDomain.waitSampling()
            dut.io.txQCtrl.retryStartPulse #= false

            // Wait random cycles
            dut.clockDomain.waitSampling(5)
            // No normal WR wait for DMA read responses, stop retry flush
            dut.io.sqHasPendingDmaReadReq #= false
            dut.clockDomain.waitSampling()
            dut.io.sqHasPendingDmaReadReq #= true

            dut.clockDomain.waitSamplingWhere(
              inputRetryWorkReqQueue.isEmpty && expectedOutputQueue.isEmpty
            )

            // Generate retry done pulse
            dut.io.retryScanCtrlBus.donePulse #= true
            dut.clockDomain.waitSampling()
            dut.io.retryScanCtrlBus.donePulse #= false

            println(f"${simTime()} time: retry WR process done")
//            println(f"${simTime()} time: should change to SqSubState.NORMAL state, dut.io.retryClear.retryFlushDone=${dut.io.retryClear.retryFlushDone.toBoolean}")

            retryWorkReqGenQueue.clear()
            retryOutputQueue.clear()
          }
        }
      }

      var normalReqPsnStart = INIT_PSN
      streamMasterPayloadFromQueueNoRandomDelay(
        dut.io.workReq,
        dut.clockDomain,
        inputWorkReqQueue,
        payloadAssignFunc = (workReq: WorkReq, payloadData: WorkReqData) => {
          val (
            workReqId,
            workReqOpCode,
            remoteAddr,
            rKey,
            ackReq,
            atomicSwap,
            atomicComp,
            immData,
            localAddr,
            pktLen,
            lKey
          ) = payloadData
          workReq.id #= workReqId
          workReq.opcode #= workReqOpCode
          workReq.raddr #= remoteAddr
          workReq.rkey #= rKey
          workReq.ackReq #= ackReq
          workReq.swap #= atomicSwap
          workReq.comp #= atomicComp
          workReq.immDtOrRmtKeyToInv #= immData
          workReq.laddr #= localAddr
          workReq.lenBytes #= pktLen
          workReq.lkey #= lKey

          val reqValid = true
          reqValid
        }
      )
      onStreamFire(dut.io.workReq, dut.clockDomain) {
        val workReqOpCode = dut.io.workReq.opcode.toEnum
        val workReqLenBytes = dut.io.workReq.lenBytes.toLong

        val (expectedPktFragQueue, lastPsn, reqPktNum, respPktNum) =
          RdmaDataPktSim.rdmaReqPktFragGenFromWorkReq(
            dut.io.workReq,
            normalReqPsnStart,
            pmtuLen,
            busWidth
          )

        val normalOutputQueue = expectedPktFragQueue.map { pktFragGenData =>
          val (psn, fragLast, _, _, _, padCnt, mty, _, opcode) = pktFragGenData
          (psn, opcode, padCnt, mty, fragLast)
        }
        expectedOutputQueue.appendAll(normalOutputQueue)

        if (!normalReqOrRetryReq) {
          retryWorkReqGenQueue.enqueue(
            (
              dut.io.workReq.id.toBigInt,
              workReqOpCode,
              dut.io.workReq.raddr.toBigInt,
              dut.io.workReq.rkey.toLong,
              dut.io.workReq.ackReq.toBoolean,
              dut.io.workReq.swap.toBigInt,
              dut.io.workReq.comp.toBigInt,
              dut.io.workReq.immDtOrRmtKeyToInv.toLong,
              dut.io.workReq.laddr.toBigInt,
              workReqLenBytes,
              dut.io.workReq.lkey.toLong,
              normalReqPsnStart,
              if (workReqOpCode.isReadReq()) { respPktNum }
              else { reqPktNum }
            )
          )

          retryOutputQueue.appendAll(normalOutputQueue)
        }

        normalReqPsnStart = lastPsn +% 1
      }

      if (normalReqOrRetryReq) {
        dut.io.retryWorkReq.valid #= false
      } else {
        streamMasterPayloadFromQueueNoRandomDelay(
          dut.io.retryWorkReq,
          dut.clockDomain,
          inputRetryWorkReqQueue,
          payloadAssignFunc =
            (retryWorkReq: RetryWorkReq, payloadData: RetryWorkReqData) => {
              val (
                workReqId,
                workReqOpCode,
                remoteAddr,
                rKey,
                ackReq,
                atomicSwap,
                atomicComp,
                immData,
                localAddr,
                pktLen,
                lKey,
                psnStart,
                pktNum
              ) = payloadData

              retryWorkReq.cachedWorkReq.workReq.id #= workReqId
              retryWorkReq.cachedWorkReq.workReq.opcode #= workReqOpCode
              retryWorkReq.cachedWorkReq.workReq.raddr #= remoteAddr
              retryWorkReq.cachedWorkReq.workReq.rkey #= rKey
              retryWorkReq.cachedWorkReq.workReq.ackReq #= ackReq
              retryWorkReq.cachedWorkReq.workReq.swap #= atomicSwap
              retryWorkReq.cachedWorkReq.workReq.comp #= atomicComp
              retryWorkReq.cachedWorkReq.workReq.immDtOrRmtKeyToInv #= immData
              retryWorkReq.cachedWorkReq.workReq.laddr #= localAddr
              retryWorkReq.cachedWorkReq.workReq.lenBytes #= pktLen
              retryWorkReq.cachedWorkReq.workReq.lkey #= lKey
              retryWorkReq.cachedWorkReq.psnStart #= psnStart
              retryWorkReq.cachedWorkReq.pktNum #= pktNum
              if (retryExceeded) {
                retryWorkReq.rnrCnt #= 1
                retryWorkReq.retryCnt #= 1
              } else {
                retryWorkReq.rnrCnt #= DEFAULT_MAX_RNR_RETRY_CNT
                retryWorkReq.retryCnt #= DEFAULT_MAX_RETRY_CNT
              }

              val reqValid = true
              reqValid
            }
        )
      }

      streamSlaveAlwaysReady(dut.io.workReqCachePush, dut.clockDomain)
      onStreamFire(dut.io.workReqCachePush, dut.clockDomain) {
        // TODO: record normal WR
      }

      streamSlaveAlwaysReady(dut.io.workCompErr, dut.clockDomain)
      onStreamFire(dut.io.workCompErr, dut.clockDomain) {
        // TODO: check WC error
      }

      streamSlaveAlwaysReady(dut.io.tx.pktFrag, dut.clockDomain)
      onStreamFire(dut.io.tx.pktFrag, dut.clockDomain) {
        actualOutputQueue.enqueue(
          (
            dut.io.tx.pktFrag.bth.psn.toInt,
            OpCode(dut.io.tx.pktFrag.bth.opcodeFull.toInt),
            dut.io.tx.pktFrag.bth.padCnt.toInt,
            dut.io.tx.pktFrag.mty.toBigInt,
            dut.io.tx.pktFrag.last.toBoolean
          )
        )
      }

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        expectedOutputQueue,
        actualOutputQueue,
        MATCH_CNT
      )
    }
}

class RetryHandlerTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U1024

  val simCfg = SimConfig.allOptimisation.withWave
    .compile(new RetryHandler)

  def randomRetryStartPsn(psnStart: PsnStart, pktNum: PktNum): PSN = {
    // RDMA max packet length 2GB=2^31
    psnStart +% scala.util.Random.nextInt(pktNum)
  }

  def testFunc(isRetryOverLimit: Boolean, isPartialRetry: Boolean): Unit =
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      val retryLimit = 3
      dut.io.qpAttr.maxRetryCnt #= retryLimit
      dut.io.qpAttr.pmtu #= pmtuLen.id
      dut.io.retryScanCtrlBus.donePulse #= false
      if (isPartialRetry) {
        dut.io.qpAttr.retryReason #= RetryReason.SEQ_ERR
      } else {
        dut.io.qpAttr.retryReason #= RetryReason.RNR
      }
      dut.io.txQCtrl.wrongStateFlush #= false

      val inputQueue = mutable.Queue[
        (
            PhysicalAddr,
            PsnStart,
            PktNum,
            PktLen,
            SpinalEnumElement[WorkReqOpCode.type],
            VirtualAddr,
            VirtualAddr,
            LRKey
        )
      ]()
      val outputQueue = mutable.Queue[
        (
            PhysicalAddr,
            PsnStart,
            PktNum,
            PktLen,
            SpinalEnumElement[WorkReqOpCode.type],
            VirtualAddr,
            VirtualAddr,
            LRKey
        )
      ]()

      var nextPsn = 0
      streamMasterDriver(dut.io.retryWorkReqIn, dut.clockDomain) {
        val curPsn = nextPsn
        dut.io.retryWorkReqIn.cachedWorkReq.psnStart #= curPsn
        val workReqOpCode = WorkReqSim.randomRdmaReqOpCode()
        dut.io.retryWorkReqIn.cachedWorkReq.workReq.opcode #= workReqOpCode
        val pktLen = if (workReqOpCode.isAtomicReq()) {
          ATOMIC_DATA_LEN.toLong
        } else {
          WorkReqSim.randomDmaLength()
        }
        dut.io.retryWorkReqIn.cachedWorkReq.workReq.lenBytes #= pktLen
        val pktNum = MiscUtils.computePktNum(pktLen, pmtuLen)
        dut.io.retryWorkReqIn.cachedWorkReq.pktNum #= pktNum

        val retryStartPsn = if (isPartialRetry) {
          randomRetryStartPsn(curPsn, pktNum)
        } else {
          curPsn
        }
        nextPsn = nextPsn +% pktNum
        dut.io.qpAttr.retryStartPsn #= retryStartPsn
        dut.io.qpAttr.npsn #= nextPsn

        if (isRetryOverLimit) {
          dut.io.retryWorkReqIn.rnrCnt #= retryLimit + 1
          dut.io.retryWorkReqIn.retryCnt #= retryLimit + 1
        } else {
          dut.io.retryWorkReqIn.rnrCnt #= 0
          dut.io.retryWorkReqIn.retryCnt #= 0
        }

        val workReqPsnStart = curPsn
        val pa = dut.io.retryWorkReqIn.cachedWorkReq.pa.toBigInt
        val rmtAddr = dut.io.retryWorkReqIn.cachedWorkReq.workReq.raddr.toBigInt
        val localAddr =
          dut.io.retryWorkReqIn.cachedWorkReq.workReq.laddr.toBigInt
        val rmtKey = dut.io.retryWorkReqIn.cachedWorkReq.workReq.rkey.toLong
        val (
          retryWorkReqPsnStart,
          retryWorkReqLenBytes,
          retryWorkReqPhysicalAddr,
          retryWorkReqRmtAddr,
          retryWorkReqLocalAddr,
          retryWorkReqPktNum
        ) = if (isPartialRetry) {
          val psnDiff = PsnSim.psnDiff(retryStartPsn, workReqPsnStart)
          val pktLenDiff = psnDiff << pmtuLen.id
          (
            retryStartPsn,
            pktLen - pktLenDiff,
            pa + pktLenDiff,
            rmtAddr + pktLenDiff,
            localAddr + pktLenDiff,
            pktNum - psnDiff
          )
        } else {
          (workReqPsnStart, pktLen, pa, rmtAddr, localAddr, pktNum)
        }
//          println(
//            f"${simTime()} time: nPSN=${nextPsn}%X, retryStartPsn=${retryStartPsn}%X=${retryStartPsn}, workReqPsnStart=${workReqPsnStart}%X=${workReqPsnStart}, pmtuLen=${pmtuLen.id}%X, pktLen=${pktLen}%X=${pktLen}, pa=${pa}%X=${pa}, rmtAddr=${rmtAddr}%X=${rmtAddr}, retryWorkReqLenBytes=${retryWorkReqLenBytes}%X=${retryWorkReqLenBytes}, retryWorkReqPhysicalAddr=${retryWorkReqPhysicalAddr}%X=${retryWorkReqPhysicalAddr}, retryWorkReqRmtAddr=${retryWorkReqRmtAddr}%X=${retryWorkReqRmtAddr}, retryWorkReqPktNum=${retryWorkReqPktNum}%X=${retryWorkReqPktNum}"
//          )
        inputQueue.enqueue(
          (
            retryWorkReqPhysicalAddr,
            retryWorkReqPsnStart,
            retryWorkReqPktNum,
            retryWorkReqLenBytes,
            workReqOpCode,
            retryWorkReqRmtAddr,
            retryWorkReqLocalAddr,
            rmtKey
          )
        )
      }

      streamSlaveAlwaysReady(dut.io.retryWorkReqOut, dut.clockDomain)
      onStreamFire(dut.io.retryWorkReqOut, dut.clockDomain) {
        outputQueue.enqueue(
          (
            dut.io.retryWorkReqOut.pa.toBigInt,
            dut.io.retryWorkReqOut.psnStart.toInt,
            dut.io.retryWorkReqOut.pktNum.toInt,
            dut.io.retryWorkReqOut.workReq.lenBytes.toLong,
            dut.io.retryWorkReqOut.workReq.opcode.toEnum,
            dut.io.retryWorkReqOut.workReq.raddr.toBigInt,
            dut.io.retryWorkReqOut.workReq.laddr.toBigInt,
            dut.io.retryWorkReqOut.workReq.rkey.toLong
          )
        )
      }

      if (isRetryOverLimit) {
        MiscUtils.checkSignalWhen(
          dut.clockDomain,
          when = dut.io.retryWorkReqIn.valid.toBoolean,
          signal = dut.io.errNotifier.pulse.toBoolean,
          clue =
            f"${simTime()} time: dut.io.errNotifier.pulse=${dut.io.errNotifier.pulse.toBoolean} should be true when dut.io.retryWorkReqIn.valid=${dut.io.retryWorkReqIn.valid.toBoolean}"
        )
      }

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        inputQueue,
        outputQueue,
        MATCH_CNT
      )
    }

  test("RetryHandler normal case") {
    testFunc(isRetryOverLimit = false, isPartialRetry = false)
  }

  test("RetryHandler partial retry case") {
    // TODO: verify retry counter increment
    testFunc(isRetryOverLimit = false, isPartialRetry = true)
  }

  test("RetryHandler retry limit exceed case") {
    testFunc(isRetryOverLimit = true, isPartialRetry = true)
  }
}
