package rdma

import spinal.core._
import spinal.core.sim._
import spinal.lib._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import org.scalatest.AppendedClues._

import ConstantSettings._
import RdmaConstants._
import StreamSimUtil._
import AckTypeSim._
import AethSim._
import BthSim._
import OpCodeSim._
import PsnSim._
import RdmaTypeReDef._
import WorkReqSim._
import SimSettings._

import scala.collection.mutable
import scala.language.postfixOps

class CoalesceAndNormalAndRetryNakHandlerTest extends AnyFunSuite {
  val busWidth = BusWidth.W512

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(
      SpinalConfig(defaultClockDomainFrequency = FixedFrequency(200 MHz))
    )
    .compile(new CoalesceAndNormalAndRetryNakHandler(busWidth))

// TODO: test("CoalesceAndNormalAndRetryNakHandler error flush test")
// TODO: test("CoalesceAndNormalAndRetryNakHandler implicit retry test")
// TODO: test("CoalesceAndNormalAndRetryNakHandler response timeout test")

  test("CoalesceAndNormalAndRetryNakHandler duplicate and ghost ACK test") {
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      dut.io.qpAttr.dqpn #= 11
      dut.io.qpAttr.respTimeOut #= INFINITE_RESP_TIMEOUT // Disable response timeout retry

      // TODO: change flush signal accordingly
      dut.io.txQCtrl.wrongStateFlush #= false
      dut.io.txQCtrl.errorFlush #= false
      dut.io.txQCtrl.retryFlush #= false

      val pmtuLen = PMTU.U256
      val (_, pktNumItr, psnItr, totalLenItr) =
        SendWriteReqReadRespInputGen.getItr(pmtuLen, busWidth)

      val dupOrGhostAckQueue = mutable.Queue[(PktNum, PsnStart, PktLen)]()
      val inputAckQueue = mutable.Queue[PSN]()

      val pendingReqNum = MAX_PENDING_WORK_REQ_NUM
      val matchCnt = MATCH_CNT

      for (_ <- 0 until pendingReqNum) {
        val pktNum = pktNumItr.next()
        val psnStart = psnItr.next()
        val totalLenBytes = totalLenItr.next()
        dupOrGhostAckQueue.enqueue((pktNum, psnStart, totalLenBytes.toLong))
      }
      fork {
        while (true) {
          dut.clockDomain.waitSamplingWhere(dupOrGhostAckQueue.isEmpty)

          for (_ <- 0 until pendingReqNum) {
            val pktNum = pktNumItr.next()
            val psnStart = psnItr.next()
            val totalLenBytes = totalLenItr.next()
            dupOrGhostAckQueue.enqueue((pktNum, psnStart, totalLenBytes.toLong))
          }
        }
      }

      // io.cachedWorkReqPop never fire
      MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
        cond = !dut.io.cachedWorkReqPop.ready.toBoolean,
//        !(dut.io.cachedWorkReqPop.valid.toBoolean && dut.io.cachedWorkReqPop.ready.toBoolean),
        clue =
          f"${simTime()} time: dut.io.cachedWorkReqPop.ready=${dut.io.cachedWorkReqPop.ready.toBoolean} should be false for duplicate or ghost ACK"
      )

      streamMasterDriver(dut.io.rx, dut.clockDomain) {
        val (pktNum, psnStart, totalLenBytes) = dupOrGhostAckQueue.dequeue()

        // Set input to dut.io.cachedWorkReqPop
        val randOpCode = WorkReqSim.randomSendWriteOpCode()
        dut.io.cachedWorkReqPop.workReq.opcode #= randOpCode
        // NOTE: if PSN comparison is involved, it must update nPSN too
        dut.io.qpAttr.npsn #= psnStart +% pktNum
        dut.io.cachedWorkReqPop.pktNum #= pktNum
        dut.io.cachedWorkReqPop.psnStart #= psnStart
        dut.io.cachedWorkReqPop.workReq.lenBytes #= totalLenBytes

        // Set input to dut.io.rx
        val ackPsn = psnStart -% 1
        dut.io.rx.pktFrag.bth.psn #= ackPsn
        dut.io.rx.pktFrag.bth
          .setTransportAndOpCode(Transports.RC, OpCode.ACKNOWLEDGE)
        dut.io.rx.aeth.setAsNormalAck()
//        println(
//          f"${simTime()} time: ACK PSN=${ackPsn}=${ackPsn}%X, psnStart=${psnStart}=${psnStart}%X, pktNum=${pktNum}=${pktNum}%X"
//        )
      }
      onStreamFire(dut.io.rx, dut.clockDomain) {
        inputAckQueue.enqueue(dut.io.rx.pktFrag.bth.psn.toInt)
      }

      streamSlaveRandomizer(
        dut.io.cachedWorkReqAndRespWithAeth,
        dut.clockDomain
      )
      // io.cachedWorkReqAndRespWithAeth never valid
      MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
        !dut.io.cachedWorkReqAndRespWithAeth.valid.toBoolean,
        f"${simTime()} time: dut.io.cachedWorkReqAndRespWithAeth.valid=${dut.io.cachedWorkReqAndRespWithAeth.valid.toBoolean} should be false for duplicate or ghost ACK"
      )

      waitUntil(inputAckQueue.size > matchCnt)
    }
  }

  test(
    "CoalesceAndNormalAndRetryNakHandler normal ACK, explicit retry NAK, fatal NAK test"
  ) {
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      dut.io.qpAttr.dqpn #= 11
      dut.io.qpAttr.respTimeOut #= 0 // Disable response timeout retry

      // TODO: change flush signal accordingly
      dut.io.txQCtrl.wrongStateFlush #= false
      dut.io.txQCtrl.errorFlush #= false
      dut.io.txQCtrl.retryFlush #= false

      val pmtuLen = PMTU.U1024
      val (_, pktNumItr, psnStartItr, totalLenItr) =
        SendWriteReqReadRespInputGen.getItr(pmtuLen, busWidth)

      val cachedWorkReqQueue = mutable.Queue[(PktNum, PsnStart, PktLen)]()
      val explicitAckQueue =
        mutable.Queue[(PktNum, PsnStart, SpinalEnumElement[AckType.type])]()
      val inputCachedWorkReqQueue = mutable.Queue[
        (
            PktNum,
            PsnStart,
            WorkReqId,
            PktLen,
            SpinalEnumElement[WorkReqOpCode.type]
        )
      ]()
      val inputAckQueue =
        mutable.Queue[(PSN, SpinalEnumElement[AckType.type])]()
      val outputWorkReqAndAckQueue =
        mutable.Queue[
          (
              PSN,
//              WorkReqFlags, // SpinalEnumElement[WorkCompFlags.type],
              PktLen,
              SpinalEnumElement[WorkReqOpCode.type],
              WorkReqId,
              SpinalEnumElement[WorkCompStatus.type]
          )
        ]()
      val matchQueue = mutable.Queue[Int]()

      val pendingReqNum = MAX_PENDING_WORK_REQ_NUM
      val matchCnt = MATCH_CNT
      for (idx <- 0 until pendingReqNum) {
        val pktNum = pktNumItr.next()
        val psnStart = psnStartItr.next()
        val lenBytes = totalLenItr.next()
        // NOTE: if PSN comparison is involved, it must update nPSN too
        dut.io.qpAttr.npsn #= psnStart +% pktNum
        cachedWorkReqQueue.enqueue((pktNum, psnStart, lenBytes.toLong))
//        println(
//          f"${simTime()} time: cachedWorkReqQueue enqueue: pktNum=${pktNum}%X, psnStart=${psnStart}%X, lenBytes=${lenBytes}%X"
//        )
        if ((idx % pendingReqNum) == (pendingReqNum - 1)) {
          val retryNakType = AckTypeSim.randomRetryNak()
          val normalAckOrFatalNakType = AckTypeSim.randomNormalAckOrFatalNak()
          explicitAckQueue.enqueue((pktNum, psnStart, retryNakType))
          explicitAckQueue.enqueue((pktNum, psnStart, normalAckOrFatalNakType))
//          println(
//            f"${simTime()} time: explicitAckQueue enqueue: pktNum=${pktNum}=${pktNum}%X, psnStart=${psnStart}=${psnStart}%X, retryNakType=${retryNakType}, normalAckOrFatalNakeType=${normalAckOrFatalNakeType}"
//          )
        }
      }
      fork {
        while (true) {
          dut.clockDomain.waitSamplingWhere(explicitAckQueue.isEmpty)

          for (idx <- 0 until pendingReqNum) {
            val pktNum = pktNumItr.next()
            val psnStart = psnStartItr.next()
            val lenBytes = totalLenItr.next()
            // NOTE: if PSN comparison is involved, it must update nPSN too
            dut.io.qpAttr.npsn #= psnStart +% pktNum
            cachedWorkReqQueue.enqueue((pktNum, psnStart, lenBytes.toLong))
            if ((idx % pendingReqNum) == (pendingReqNum - 1)) {
              val retryNakType = AckTypeSim.randomRetryNak()
              val normalAckOrFatalNakType =
                AckTypeSim.randomNormalAckOrFatalNak()
              explicitAckQueue.enqueue((pktNum, psnStart, retryNakType))
              explicitAckQueue.enqueue(
                (pktNum, psnStart, normalAckOrFatalNakType)
              )
//              println(
//                f"${simTime()} time: explicitAckQueue enqueue: pktNum=${pktNum}=${pktNum}%X, psnStart=${psnStart}=${psnStart}%X, ackType=${AckType.NAK_RNR}"
//              )
            }
          }
        }
      }

      // NOTE: io.cachedWorkReqPop.valid should be always true,
      // otherwise ACKs will be treated as ghost ones
      streamMasterDriverAlwaysValid(dut.io.cachedWorkReqPop, dut.clockDomain) {
        val (pktNum, psnStart, lenBytes) = cachedWorkReqQueue.dequeue()
        val randOpCode = WorkReqSim.randomSendWriteOpCode()
        dut.io.cachedWorkReqPop.workReq.opcode #= randOpCode
        dut.io.cachedWorkReqPop.pktNum #= pktNum
        dut.io.cachedWorkReqPop.psnStart #= psnStart
        dut.io.cachedWorkReqPop.workReq.lenBytes #= lenBytes
//        println(
//          f"${simTime()} time: WR: opcode=${randOpCode}, pktNum=${pktNum}=${pktNum}%X, psnStart=${psnStart}=${psnStart}%X, lenBytes=${lenBytes}=${lenBytes}%X"
//        )
      }
      onStreamFire(dut.io.cachedWorkReqPop, dut.clockDomain) {
        inputCachedWorkReqQueue.enqueue(
          (
            dut.io.cachedWorkReqPop.pktNum.toInt,
            dut.io.cachedWorkReqPop.psnStart.toInt,
            dut.io.cachedWorkReqPop.workReq.id.toBigInt,
            dut.io.cachedWorkReqPop.workReq.lenBytes.toLong,
            dut.io.cachedWorkReqPop.workReq.opcode.toEnum
          )
        )
      }

      streamMasterDriver(dut.io.rx, dut.clockDomain) {
        val (pktNum, psnStart, ackType) = explicitAckQueue.dequeue()
        val ackPsn = psnStart +% pktNum -% 1
//        dut.io.qpAttr.npsn #= ackPsn
        dut.io.rx.pktFrag.bth.psn #= ackPsn
        dut.io.rx.pktFrag.bth
          .setTransportAndOpCode(Transports.RC, OpCode.ACKNOWLEDGE)
        dut.io.rx.aeth.setAs(ackType)
        dut.io.rx.workCompStatus #= AckTypeSim.toWorkCompStatus(ackType)
//        println(
//          f"${simTime()} time: ACK ackType=${ackType} PSN=${ackPsn}=${ackPsn}%X, psnStart=${psnStart}=${psnStart}%X, pktNum=${pktNum}=${pktNum}%X"
//        )
      }
      onStreamFire(dut.io.rx, dut.clockDomain) {
        val ackType = AckTypeSim.decodeFromAeth(dut.io.rx.aeth)
        if (ackType.isRetryNak()) {
          dut.io.retryNotifier.pulse.toBoolean shouldBe true withClue
            f"${simTime()} time: dut.io.retryNotifier.pulse=${dut.io.retryNotifier.pulse.toBoolean} should be true when ackType=${ackType}"
        }
        inputAckQueue.enqueue((dut.io.rx.pktFrag.bth.psn.toInt, ackType))
      }

      streamSlaveRandomizer(
        dut.io.cachedWorkReqAndRespWithAeth,
        dut.clockDomain
      )
      onStreamFire(dut.io.cachedWorkReqAndRespWithAeth, dut.clockDomain) {
        outputWorkReqAndAckQueue.enqueue(
          (
            dut.io.cachedWorkReqAndRespWithAeth.pktFrag.bth.psn.toInt,
//            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.flags.flagBits.toInt,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.lenBytes.toLong,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.opcode.toEnum,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.id.toBigInt,
            dut.io.cachedWorkReqAndRespWithAeth.workCompStatus.toEnum
          )
        )
      }

      fork {
        while (true) {
          val (retryNakPsnIn, retryNakTypeIn) =
            MiscUtils.safeDeQueue(inputAckQueue, dut.clockDomain)
          val (ackPsnIn, ackTypeIn) =
            MiscUtils.safeDeQueue(inputAckQueue, dut.clockDomain)

          retryNakPsnIn shouldBe ackPsnIn withClue
            f"${simTime()} time: retryNakPsnIn=${retryNakPsnIn} == ackPsnIn=${ackPsnIn}"

          retryNakTypeIn.isRetryNak() shouldBe true withClue
            f"${simTime()} time: retryNakTypeIn=${retryNakTypeIn} should be retry NAK"

          ackTypeIn.isFatalNak() || ackTypeIn
            .isNormalAck() shouldBe true withClue
            f"${simTime()} time: ackTypeIn=${ackTypeIn} should be normal ACK or fatal NAK"
//          println(
//            f"${simTime()} time: ACK psnIn=${ackPsnIn}=${ackPsnIn}%X, ackTypeIn=${ackTypeIn}"
//          )

          var needCoalesceAck = false
          do {
            val (
              pktNumIn,
              psnStartInIn,
              workReqIdIn,
              lenBytesIn,
              workReqOpCodeIn
            ) = MiscUtils.safeDeQueue(inputCachedWorkReqQueue, dut.clockDomain)
            val (
              ackPsnOut,
//              workReqFlagOut,
              lenBytesOut,
              workReqOpCodeOut,
              workReqIdOut,
              workCompStatusOut
            ) =
              MiscUtils.safeDeQueue(outputWorkReqAndAckQueue, dut.clockDomain)
            val workReqEndPsn = psnStartInIn +% pktNumIn -% 1
//            println(
//              f"${simTime()} time: WR: workReqOpCodeIn=${workReqOpCodeIn}, psnStartInIn=${psnStartInIn}=${psnStartInIn}%X, workReqEndPsn=${workReqEndPsn}=${workReqEndPsn}%X, ackPsnOut=${ackPsnOut}=${ackPsnOut}%X, pktNumIn=${pktNumIn}=${pktNumIn}%X"
//            )

            workReqOpCodeOut shouldBe workReqOpCodeIn withClue
              f"${simTime()} time: workReqOpCodeIn=${workReqOpCodeIn} should equal workReqOpCodeOut=${workReqOpCodeOut}"
//            WorkCompSim.sqCheckWorkCompOpCode(
//              workReqOpCodeIn,
//              workReqOpCodeOut
//            )
//            WorkCompSim.sqCheckWorkCompFlag(workReqOpCodeIn, workCompFlagOut)

            if (ackPsnOut != ackPsnIn) {
              println(
                f"${simTime()} time: ackPsnIn=${ackPsnIn}%X should equal ackPsnOut=${ackPsnOut}%X"
              )
              println(
                f"${simTime()} time: WR: workReqOpCodeIn=${workReqOpCodeIn}, psnStartInIn=${psnStartInIn}=${psnStartInIn}%X, workReqEndPsn=${workReqEndPsn}=${workReqEndPsn}%X, ackPsnOut=${ackPsnOut}=${ackPsnOut}%X, pktNumIn=${pktNumIn}=${pktNumIn}%X"
              )
            }

            ackPsnOut shouldBe ackPsnIn withClue
              f"${simTime()} time: ackPsnIn=${ackPsnIn}%X should equal ackPsnOut=${ackPsnOut}%X"

            workReqIdOut shouldBe workReqIdIn withClue
              f"${simTime()} time: workReqIdOut=${workReqIdOut}%X should equal workReqIdIn=${workReqIdIn}%X"

            lenBytesOut shouldBe lenBytesIn withClue
              f"${simTime()} time: lenBytesOut=${lenBytesOut}%X should equal lenBytesIn=${lenBytesIn}%X"

            if (workReqEndPsn != ackPsnIn) { // Coalesce ACK case

              workCompStatusOut shouldBe WorkCompStatus.SUCCESS withClue
                f"${simTime()} time: ackTypeIn=${ackTypeIn} workCompStatusOut=${workCompStatusOut} should be WorkCompStatus.SUCCESS, ackPsnIn=${ackPsnIn}%X"
            } else { // Explicit ACK case
              if (ackTypeIn.isFatalNak()) {
                workCompStatusOut shouldNot be(WorkCompStatus.SUCCESS) withClue
                  f"${simTime()} time: ackTypeIn=${ackTypeIn} workCompStatusOut=${workCompStatusOut} should not be WorkCompStatus.SUCCESS, ackPsnIn=${ackPsnIn}%X"
              } else {
                workCompStatusOut shouldBe WorkCompStatus.SUCCESS withClue
                  f"${simTime()} time: ackTypeIn=${ackTypeIn} workCompStatusOut=${workCompStatusOut} should be WorkCompStatus.SUCCESS, ackPsnIn=${ackPsnIn}%X"
              }
            }

            needCoalesceAck = workReqEndPsn != ackPsnIn
          } while (needCoalesceAck)

          matchQueue.enqueue(ackPsnIn)
        }
      }
      waitUntil(matchQueue.size > matchCnt)
    }
  }
}

class ReadRespLenCheckTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U256
  val maxFragNum = 137

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(SpinalConfig(anonymSignalPrefix = "tmp"))
    .compile {
      val dut = new ReadRespLenCheck(busWidth)
      dut.totalLenValid.simPublic()
      dut.totalLenBytes.simPublic()
      dut
    }

  test("ReadRespLenCheck read response check normal case") {
    testReadRespLenCheckFunc(lenCheckPass = true)
  }

  test("ReadRespLenCheck read response check failure case") {
    testReadRespLenCheckFunc(lenCheckPass = false)
  }

  test("ReadRespLenCheck non-read response case") {
    testNonReadRespFunc()
  }

  def testNonReadRespFunc(): Unit = simCfg.doSim { dut =>
    dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

    dut.io.txQCtrl.wrongStateFlush #= false
    dut.io.qpAttr.pmtu #= pmtuLen.id

    val inputQueue = mutable
      .Queue[(WorkReqId, PktLen, SpinalEnumElement[WorkCompStatus.type])]()
    val outputQueue = mutable
      .Queue[(WorkReqId, PktLen, SpinalEnumElement[WorkCompStatus.type])]()

    streamMasterDriver(dut.io.cachedWorkReqAndRespWithAethIn, dut.clockDomain) {
      val opcode = OpCodeSim.randomNonReadRespOpCode()
      dut.io.cachedWorkReqAndRespWithAethIn.pktFrag.bth.opcodeFull #= opcode.id
      dut.io.cachedWorkReqAndRespWithAethIn.last #= true
    }
    onStreamFire(dut.io.cachedWorkReqAndRespWithAethIn, dut.clockDomain) {
      inputQueue.enqueue(
        (
          dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.workReq.id.toBigInt,
          dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.workReq.lenBytes.toLong,
          dut.io.cachedWorkReqAndRespWithAethIn.workCompStatus.toEnum
        )
      )
    }

    streamSlaveRandomizer(
      dut.io.cachedWorkReqAndRespWithAethOut,
      dut.clockDomain
    )
    onStreamFire(dut.io.cachedWorkReqAndRespWithAethOut, dut.clockDomain) {
      outputQueue.enqueue(
        (
          dut.io.cachedWorkReqAndRespWithAethOut.cachedWorkReq.workReq.id.toBigInt,
          dut.io.cachedWorkReqAndRespWithAethOut.cachedWorkReq.workReq.lenBytes.toLong,
          dut.io.cachedWorkReqAndRespWithAethOut.workCompStatus.toEnum
        )
      )
    }

    MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
      !dut.totalLenValid.toBoolean,
      f"${simTime()} time, dut.totalLenValid=${dut.totalLenValid.toBoolean} should be false for non-read responses"
    )

    MiscUtils.checkExpectedOutputMatch(
      dut.clockDomain,
      inputQueue,
      outputQueue,
      MATCH_CNT
    )
  }

  def makeErrLen(pktLen: PktLen): PktLen = pktLen + 1
  def testReadRespLenCheckFunc(lenCheckPass: Boolean): Unit = simCfg.doSim {
    dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      dut.io.txQCtrl.wrongStateFlush #= false
      dut.io.qpAttr.pmtu #= pmtuLen.id

      val inputQueue = mutable
        .Queue[(WorkReqId, PktLen, SpinalEnumElement[WorkCompStatus.type])]()
      val outputQueue = mutable
        .Queue[(WorkReqId, PktLen, SpinalEnumElement[WorkCompStatus.type])]()

//      val (totalFragNumItr, pktNumItr, psnStartItr, totalLenItr) =
//        SendWriteReqReadRespInputGen.getItr(maxFragNum, pmtuLen, busWidth)

      RdmaDataPktSim.readRespPktFragStreamMasterDriver(
        dut.io.cachedWorkReqAndRespWithAethIn,
        dut.clockDomain,
        getRdmaPktDataFunc =
          (cachedWorkReqAndRespWithAethIn: CachedWorkReqAndRespWithAeth) =>
            cachedWorkReqAndRespWithAethIn.pktFrag,
        pmtuLen = pmtuLen,
        busWidth = busWidth,
        maxFragNum = maxFragNum,
        innerLoopFunc = (
            _, // psn,
            psnStart,
            _, // fragLast,
            _, // fragIdx,
            _, // pktFragNum,
            _, // pktIdx,
            _, // reqPktNum,
            _, // respPktNum,
            payloadLenBytes,
            _, // headerLenBytes,
            _ // opcode
        ) => {
          dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.psnStart #= psnStart
          dut.io.cachedWorkReqAndRespWithAethIn.workCompStatus #= WorkCompStatus.SUCCESS
          if (lenCheckPass) {
            dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.workReq.lenBytes #= payloadLenBytes
          } else {
            dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.workReq.lenBytes #=
              makeErrLen(payloadLenBytes)
          }
//        println(
//          f"${simTime()} time: fragIdx=${fragIdx}, fragNum=${fragNum}, isLastInputFrag=${isLastInputFrag}, isLastFragPerPkt=${isLastFragPerPkt}, fragLast=${fragLast}, totalLenBytes=${totalLenBytes}, pktNum=${pktNum}, mtyWidth=${mtyWidth}, residue=${totalLenBytes % mtyWidth}, mty=${mty}%X"
//        )
        }
      )
      onStreamFire(dut.io.cachedWorkReqAndRespWithAethIn, dut.clockDomain) {
        val expectedWorkCompStatus = if (lenCheckPass) {
          WorkCompStatus.SUCCESS
        } else {
          WorkCompStatus.LOC_LEN_ERR
        }
        val isLastFrag = dut.io.cachedWorkReqAndRespWithAethIn.last.toBoolean
        val readRespOpCode = OpCode(
          dut.io.cachedWorkReqAndRespWithAethIn.pktFrag.bth.opcodeFull.toInt
        )
        val isLastOrOnlyReadResp = readRespOpCode.isLastOrOnlyReadRespPkt()
        if (isLastOrOnlyReadResp && isLastFrag) {
          inputQueue.enqueue(
            (
              dut.io.cachedWorkReqAndRespWithAethIn.cachedWorkReq.workReq.id.toBigInt,
              if (lenCheckPass) dut.totalLenBytes.toLong
              else makeErrLen(dut.totalLenBytes.toLong),
              expectedWorkCompStatus
            )
          )
          dut.totalLenValid.toBoolean shouldBe true withClue
            f"${simTime()} time: dut.totalLenValid=${dut.totalLenValid.toBoolean} should be true when readRespOpCode=${readRespOpCode}, isLastOrOnlyReadResp=${isLastOrOnlyReadResp}, dut.io.cachedWorkReqAndRespWithAethIn.last=${isLastFrag}"
        }
      }

      streamSlaveRandomizer(
        dut.io.cachedWorkReqAndRespWithAethOut,
        dut.clockDomain
      )
      onStreamFire(dut.io.cachedWorkReqAndRespWithAethOut, dut.clockDomain) {
        val isLastFrag = dut.io.cachedWorkReqAndRespWithAethOut.last.toBoolean
        val readRespOpCode = OpCode(
          dut.io.cachedWorkReqAndRespWithAethOut.pktFrag.bth.opcodeFull.toInt
        )
        val isLastOrOnlyReadResp = readRespOpCode.isLastOrOnlyReadRespPkt()
        if (isLastOrOnlyReadResp && isLastFrag) {
          outputQueue.enqueue(
            (
              dut.io.cachedWorkReqAndRespWithAethOut.cachedWorkReq.workReq.id.toBigInt,
              dut.io.cachedWorkReqAndRespWithAethOut.cachedWorkReq.workReq.lenBytes.toLong,
              dut.io.cachedWorkReqAndRespWithAethOut.workCompStatus.toEnum
            )
          )
        }
      }

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        inputQueue,
        outputQueue,
        MATCH_CNT
      )
  }
}

class ReadAtomicRespVerifierAndFatalNakNotifierTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U256
  val maxFragNum = 37

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(SpinalConfig(anonymSignalPrefix = "tmp"))
    .compile {
      val dut = new ReadAtomicRespVerifierAndFatalNakNotifier(busWidth)
      dut.addrCheckErr.simPublic()
      dut
    }

  def testReadRespFunc(addrCacheQuerySuccess: Boolean): Unit =
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      dut.io.txQCtrl.wrongStateFlush #= false

      val readRespPktFragQueue = mutable.Queue[
        (
            PSN,
            VirtualAddr,
            WorkReqId,
            PsnStart,
            OpCode.Value,
            PktFragData,
            FragLast
        )
      ]()
      val inputWorkReqAndAckQueue =
        mutable.Queue[
          (PSN, VirtualAddr, WorkReqId, SpinalEnumElement[WorkCompStatus.type])
        ]()
      val outputWorkReqAndAckQueue = mutable.Queue[
        (PSN, VirtualAddr, WorkReqId, SpinalEnumElement[WorkCompStatus.type])
      ]()

      val inputReadResp4DmaQueue =
        mutable.Queue[(PSN, PhysicalAddr, WorkReqId, PktFragData, FragLast)]()
      val outputReadResp4DmaQueue =
        mutable.Queue[(PSN, PhysicalAddr, WorkReqId, PktFragData, FragLast)]()

      RdmaDataPktSim.readRespPktFragStreamMasterDriverAlwaysValid(
        dut.io.cachedWorkReqAndRespWithAeth,
        dut.clockDomain,
        getRdmaPktDataFunc =
          (cachedWorkReqAndRespWithAeth: CachedWorkReqAndRespWithAeth) =>
            cachedWorkReqAndRespWithAeth.pktFrag,
        pmtuLen = pmtuLen,
        busWidth = busWidth,
        maxFragNum = maxFragNum
      ) {
        (
            _, // psn,
            psnStart,
            _, // fragLast,
            _, // fragIdx,
            _, // pktFragNum,
            _, // pktIdx,
            _, // repPktNum,
            _, // respPktNum
            _, // payloadLenBytes,
            _, // headerLenBytes,
            _ // opcode
        ) =>
          dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.psnStart #= psnStart
          dut.io.cachedWorkReqAndRespWithAeth.workCompStatus #= WorkCompStatus.SUCCESS
          dut.io.cachedWorkReqAndRespWithAeth.aeth.setAsNormalAck()
      }
      onStreamFire(dut.io.cachedWorkReqAndRespWithAeth, dut.clockDomain) {
        val isLastFrag = dut.io.cachedWorkReqAndRespWithAeth.last.toBoolean
        val opcode = OpCode(
          dut.io.cachedWorkReqAndRespWithAeth.pktFrag.bth.opcodeFull.toInt
        )
        readRespPktFragQueue.enqueue(
          (
            dut.io.cachedWorkReqAndRespWithAeth.pktFrag.bth.psn.toInt,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.laddr.toBigInt,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.workReq.id.toBigInt,
            dut.io.cachedWorkReqAndRespWithAeth.cachedWorkReq.psnStart.toInt,
            opcode,
            dut.io.cachedWorkReqAndRespWithAeth.pktFrag.data.toBigInt,
            isLastFrag
          )
        )
      }

      val addrCacheRespQueue = if (addrCacheQuerySuccess) {
        AddrCacheSim.reqStreamFixedDelayAndRespSuccess(
          dut.io.addrCacheRead,
          dut.clockDomain,
          fixedRespDelayCycles = ADDR_CACHE_QUERY_DELAY_CYCLES
        )
      } else {
        AddrCacheSim.reqStreamFixedDelayAndRespFailure(
          dut.io.addrCacheRead,
          dut.clockDomain,
          fixedRespDelayCycles = ADDR_CACHE_QUERY_DELAY_CYCLES
        )
      }
      fork {
        var physicalAddr = BigInt(0)
        var isFirstFrag = true
        while (true) {
          val (
            psn,
            laddr,
            workReqId,
            psnStart,
            opcode,
            pktFragData,
            isLastFrag
          ) = MiscUtils.safeDeQueue(readRespPktFragQueue, dut.clockDomain)

          if (opcode.isFirstOrOnlyReadRespPkt() && isFirstFrag) {
            val (psnQuery, _, _, _, pa) =
              MiscUtils.safeDeQueue(addrCacheRespQueue, dut.clockDomain)
            physicalAddr = pa
            psnStart shouldBe psnQuery withClue f"${simTime()} time: psnStart=${psnStart} should == psnQuery=${psnQuery}"
          }

          if (isLastFrag) {
            isFirstFrag = true
          } else {
            isFirstFrag = false
          }

          val expectedWorkCompStatus = if (addrCacheQuerySuccess) {
            WorkCompStatus.SUCCESS
          } else {
            WorkCompStatus.LOC_ACCESS_ERR
          }

          inputWorkReqAndAckQueue.enqueue(
            (
              psnStart,
              laddr,
              workReqId,
              expectedWorkCompStatus
            )
          )
//          println(f"${simTime()} time: inputWorkReqAndAckQueue.size=${inputWorkReqAndAckQueue.size}")

          if (addrCacheQuerySuccess) {
            inputReadResp4DmaQueue.enqueue(
              (
                psn,
                physicalAddr,
                workReqId,
                pktFragData,
                isLastFrag
              )
            )
//            println(f"${simTime()} time: inputReadResp4DmaQueue.size=${inputReadResp4DmaQueue.size}")
          }
        }
      }

      streamSlaveAlwaysReady(dut.io.cachedWorkReqAndAck, dut.clockDomain)
      onStreamFire(dut.io.cachedWorkReqAndAck, dut.clockDomain) {
        outputWorkReqAndAckQueue.enqueue(
          (
            dut.io.cachedWorkReqAndAck.cachedWorkReq.psnStart.toInt,
            dut.io.cachedWorkReqAndAck.cachedWorkReq.workReq.laddr.toBigInt,
            dut.io.cachedWorkReqAndAck.cachedWorkReq.workReq.id.toBigInt,
            dut.io.cachedWorkReqAndAck.workCompStatus.toEnum
          )
        )
//        println(f"${simTime()} time: outputWorkReqAndAckQueue.size=${outputWorkReqAndAckQueue.size}")
      }

      streamSlaveAlwaysReady(
        dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo,
        dut.clockDomain
      )
      onStreamFire(
        dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo,
        dut.clockDomain
      ) {
        outputReadResp4DmaQueue.enqueue(
          (
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pktFrag.bth.psn.toInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pa.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.workReqId.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pktFrag.data.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.last.toBoolean
          )
        )
//        println(f"${simTime()} time: outputReadResp4DmaQueue.size=${outputReadResp4DmaQueue.size}")
      }

      MiscUtils.checkCondChangeOnceAndHoldAfterwards(
        dut.clockDomain,
        cond =
          dut.io.cachedWorkReqAndRespWithAeth.valid.toBoolean && dut.io.cachedWorkReqAndRespWithAeth.ready.toBoolean,
        clue =
          f"${simTime()} time: dut.io.cachedWorkReqAndRespWithAeth.fire=${dut.io.cachedWorkReqAndRespWithAeth.valid.toBoolean && dut.io.cachedWorkReqAndRespWithAeth.ready.toBoolean} should be true always"
      )
      MiscUtils.checkCondChangeOnceAndHoldAfterwards(
        dut.clockDomain,
        cond = dut.io.cachedWorkReqAndAck.valid.toBoolean,
        clue =
          f"${simTime()} time: dut.io.cachedWorkReqAndAck.valid=${dut.io.cachedWorkReqAndAck.valid.toBoolean} should be true always"
      )
      if (addrCacheQuerySuccess) {
        MiscUtils.checkCondChangeOnceAndHoldAfterwards(
          dut.clockDomain,
          cond =
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid.toBoolean,
          clue =
            f"${simTime()} time: dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid=${dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid.toBoolean} should be true always when addrCacheQuerySuccess=${addrCacheQuerySuccess}"
        )
        MiscUtils.checkExpectedOutputMatch(
          dut.clockDomain,
          inputReadResp4DmaQueue,
          outputReadResp4DmaQueue,
          MATCH_CNT
        )
        MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
          !dut.io.errNotifier.pulse.toBoolean,
          f"${simTime()} time, dut.io.errNotifier.pulse=${dut.io.errNotifier.pulse.toBoolean} should be false, when addrCacheQuerySuccess=${addrCacheQuerySuccess}"
        )
      } else {
        MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
          !(dut.io.errNotifier.pulse.toBoolean ^ dut.addrCheckErr.toBoolean),
          f"${simTime()} time, dut.io.errNotifier.pulse=${dut.io.errNotifier.pulse.toBoolean} should == dut.io.addrCheckErr=${dut.addrCheckErr.toBoolean}, when addrCacheQuerySuccess=${addrCacheQuerySuccess}"
        )
        MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
          !dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid.toBoolean,
          f"${simTime()} time, dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid=${dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.valid.toBoolean} should be false, when addrCacheQuerySuccess=${addrCacheQuerySuccess}"
        )
      }

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        inputWorkReqAndAckQueue,
        outputWorkReqAndAckQueue,
        MATCH_CNT
      )
    }

  // TODO: test("ReadAtomicRespVerifierAndFatalNakNotifier error flush test")

  test("ReadAtomicRespVerifierAndFatalNakNotifier normal read response case") {
    testReadRespFunc(addrCacheQuerySuccess = true)
  }

  test(
    "ReadAtomicRespVerifierAndFatalNakNotifier read response AddrCache query failure case"
  ) {
    testReadRespFunc(addrCacheQuerySuccess = false)
  }
}

class ReadAtomicRespDmaReqInitiatorTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U256
  val maxFragNum = 137

  val simCfg = SimConfig.allOptimisation.withWave
    .compile(new ReadAtomicRespDmaReqInitiator(busWidth))

  test("ReadAtomicRespDmaReqInitiator normal behavior test") {
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      // TODO: change flush signal accordingly
      dut.io.txQCtrl.wrongStateFlush #= false
      dut.io.txQCtrl.errorFlush #= false
      dut.io.txQCtrl.retryFlush #= false

      // Input to DUT
//      val (totalFragNumItr, pktNumItr, psnStartItr, totalLenItr) =
//        SendWriteReqReadRespInputGen.getItr(maxFragNum, pmtuLen, busWidth)

      val rxReadRespQueue =
        mutable.Queue[(PSN, PhysicalAddr, WorkReqId, PktFragData, FragLast)]()
      val readRespDmaWriteReqQueue =
        mutable.Queue[(PSN, PhysicalAddr, WorkReqId, PktFragData, FragLast)]()

      RdmaDataPktSim.readRespPktFragStreamMasterDriver(
        dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo,
        dut.clockDomain,
        getRdmaPktDataFunc =
          (readAtomicRespWithDmaInfo: SqReadAtomicRespWithDmaInfo) =>
            readAtomicRespWithDmaInfo.pktFrag,
        pmtuLen,
        busWidth,
        maxFragNum,
        innerLoopFunc = (
            _, // psn,
            _, // psnStart,
            _, // fragLast,
            _, // fragIdx,
            _, // pktFragNum,
            _, // pktIdx,
            _, // reqPktNum,
            _, // respPktNum,
            _, // payloadLenBytes,
            _, // headerLenBytes,
            _ // opcode
        ) => {
//        println(
//          f"${simTime()} time: fragIdx=${fragIdx}, fragNum=${fragNum}, isLastInputFrag=${isLastInputFrag}, isLastFragPerPkt=${isLastFragPerPkt}, fragLast=${fragLast}, totalLenBytes=${totalLenBytes}, pktNum=${pktNum}, mtyWidth=${mtyWidth}, residue=${totalLenBytes % mtyWidth}, mty=${mty}%X"
//        )
        }
      )

      onStreamFire(
        dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo,
        dut.clockDomain
      ) {
//        println(f"${simTime()} time: dut.io.rx.pktFrag.bth.psn=${dut.io.rx.pktFrag.bth.psn.toInt}")
        rxReadRespQueue.enqueue(
          (
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pktFrag.bth.psn.toInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pa.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.workReqId.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.pktFrag.data.toBigInt,
            dut.io.readAtomicRespWithDmaInfoBus.respWithDmaInfo.last.toBoolean
          )
        )
      }

      streamSlaveRandomizer(dut.io.respDmaWrite.req, dut.clockDomain)
      onStreamFire(dut.io.respDmaWrite.req, dut.clockDomain) {
        readRespDmaWriteReqQueue.enqueue(
          (
            dut.io.respDmaWrite.req.psn.toInt,
            dut.io.respDmaWrite.req.pa.toBigInt,
            dut.io.respDmaWrite.req.workReqId.toBigInt,
            dut.io.respDmaWrite.req.data.toBigInt,
            dut.io.respDmaWrite.req.last.toBoolean
          )
        )
      }

//      streamSlaveRandomizer(dut.io.atomicRespDmaWriteReq.req, dut.clockDomain)
//      MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
//        !dut.io.atomicRespDmaWriteReq.req.valid.toBoolean,
//        f"${simTime()} time: dut.io.atomicRespDmaWriteReq.req.valid=${dut.io.atomicRespDmaWriteReq.req.valid.toBoolean} should be false, since atomic not supported"
//      )

//      streamSlaveRandomizer(dut.io.atomicRespDmaWriteReq.req, dut.clockDomain)
//      MiscUtils.checkConditionAlwaysHold(dut.clockDomain)(
//        !dut.io.atomicRespDmaWriteReq.req.valid.toBoolean,
//        f"${simTime()} time: dut.io.atomicRespDmaWriteReq.req.valid=${dut.io.atomicRespDmaWriteReq.req.valid.toBoolean} should be false, since atomic not supported"
//      )

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        rxReadRespQueue,
        readRespDmaWriteReqQueue,
        MATCH_CNT
      )
    }
  }
}

class WorkCompGenTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U256
  val maxFragNum = 137

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(SpinalConfig(anonymSignalPrefix = "tmp"))
    .compile(new WorkCompGen)

  def testFunc(
      ackType: SpinalEnumElement[AckType.type],
      needDmaWriteResp: Boolean
  ): Unit = simCfg.doSim { dut =>
    dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

    val workCompStatus = AckTypeSim.toWorkCompStatus(ackType)
    val sqpn = 11
    val dqpn = 10
    dut.io.qpAttr.sqpn #= sqpn
    dut.io.qpAttr.dqpn #= dqpn
    // TODO: change flush signal accordingly
    dut.io.txQCtrl.wrongStateFlush #= false
    dut.io.txQCtrl.errorFlush #= false
    dut.io.txQCtrl.retryFlush #= false

    val readWorkReqMetaDataQueue = DelayedQueue[
      (
          WorkReqId,
          PsnStart,
          PktLen
      )
    ](dut.clockDomain, DMA_WRITE_DELAY_CYCLES)
    val inputQueue = mutable.Queue[
      (
          WorkReqId,
          SpinalEnumElement[WorkCompOpCode.type],
          SpinalEnumElement[WorkCompStatus.type],
          PktLen,
          DQPN,
          SQPN
      )
    ]()
    val outputQueue = mutable.Queue[
      (
          WorkReqId,
          SpinalEnumElement[WorkCompOpCode.type],
          SpinalEnumElement[WorkCompStatus.type],
          PktLen,
          DQPN,
          SQPN
      )
    ]()
    val workReqMetaDataQueue = mutable.Queue[
      (
          SpinalEnumElement[WorkReqOpCode.type],
          FragNum,
          PktNum,
          PsnStart,
          PktLen
      )
    ]()

    // Input to DUT
    fork {
      val (totalFragNumItr, pktNumItr, psnStartItr, payloadLenItr) =
        SendWriteReqReadRespInputGen.getItr(maxFragNum, pmtuLen, busWidth)

      while (true) {
        dut.clockDomain.waitSampling()

        val totalFragNum = totalFragNumItr.next()
        val pktNum = pktNumItr.next()
        val psnStart = psnStartItr.next()
        val payloadLenBytes = payloadLenItr.next()

        // TODO: support atomic request
        val workReqOpCode = if (needDmaWriteResp) {
          WorkReqOpCode.RDMA_READ
        } else {
          WorkReqSim.randomSendWriteOpCode()
        }
        workReqMetaDataQueue.enqueue(
          (
            workReqOpCode,
            totalFragNum,
            pktNum,
            psnStart,
            payloadLenBytes.toLong
          )
        )

        if (workReqOpCode.isReadReq()) {
          val workReqId = psnStart
//              cachedWorkReqAndAck.cachedWorkReq.workReq.id.toBigInt
          readWorkReqMetaDataQueue.enqueue(
            (
              BigInt(workReqId),
              psnStart,
              payloadLenBytes.toLong
            )
          )
//        println(f"${simTime()} time: workReqId=${workReqId}%X, psnStart=${psnStart}%X, totalLenBytes=${totalLenBytes}%X")
        }
      }
    }

    streamMasterPayloadFromQueueNoRandomDelay(
      dut.io.cachedWorkReqAndAck,
      dut.clockDomain,
      workReqMetaDataQueue,
      payloadAssignFunc = (
          cachedWorkReqAndAck: Fragment[CachedWorkReqAndAck],
          payloadData: (
              SpinalEnumElement[WorkReqOpCode.type],
              FragNum,
              PktNum,
              PsnStart,
              PktLen
          )
      ) => {
        val (
          workReqOpCode,
          _, // totalFragNum,
          pktNum,
          psnStart,
          payloadLenBytes
        ) = payloadData

        dut.io.qpAttr.npsn #= psnStart +% pktNum

        cachedWorkReqAndAck.cachedWorkReq.workReq.id #= psnStart
        cachedWorkReqAndAck.cachedWorkReq.workReq.sqpn #= sqpn
        cachedWorkReqAndAck.cachedWorkReq.psnStart #= psnStart
        cachedWorkReqAndAck.cachedWorkReq.pktNum #= pktNum
        cachedWorkReqAndAck.cachedWorkReq.workReq.lenBytes #= payloadLenBytes
        cachedWorkReqAndAck.cachedWorkReq.workReq.flags.flagBits #=
          WorkReqSim.assignFlagBits(WorkReqSendFlagEnum.SIGNALED)
        cachedWorkReqAndAck.last #= true
        cachedWorkReqAndAck.ackValid #= true

        cachedWorkReqAndAck.cachedWorkReq.workReq.opcode #= workReqOpCode

        cachedWorkReqAndAck.ack.bth.psn #= psnStart +% (pktNum - 1)
        cachedWorkReqAndAck.ack.aeth.setAs(ackType)
        cachedWorkReqAndAck.workCompStatus #= workCompStatus

        val reqValid = true
        reqValid
      }
    )
    onStreamFire(dut.io.cachedWorkReqAndAck, dut.clockDomain) {
      val workReqOpCode =
        dut.io.cachedWorkReqAndAck.cachedWorkReq.workReq.opcode.toEnum
      val workCompOpCode = WorkCompSim.fromSqWorkReqOpCode(workReqOpCode)
      inputQueue.enqueue(
        (
          dut.io.cachedWorkReqAndAck.cachedWorkReq.workReq.id.toBigInt,
          workCompOpCode,
          workCompStatus,
          dut.io.cachedWorkReqAndAck.cachedWorkReq.workReq.lenBytes.toLong,
          dut.io.qpAttr.dqpn.toInt,
          dut.io.qpAttr.sqpn.toInt
        )
      )
    }

    streamMasterPayloadFromQueueNoRandomDelay(
      dut.io.respDmaWrite.resp,
      dut.clockDomain,
      readWorkReqMetaDataQueue.toMutableQueue(),
//        maxIntervalCycles = DMA_WRITE_DELAY_CYCLES,
      payloadAssignFunc = (
          dmaWriteResp: DmaWriteResp,
          workReqData: (
              WorkReqId,
              PsnStart,
              PktLen
          )
      ) => {
        val (workReqId, psnStart, pktLen) = workReqData
        dmaWriteResp.initiator #= DmaInitiator.SQ_WR
        dmaWriteResp.workReqId #= workReqId
        dmaWriteResp.psn #= psnStart
        dmaWriteResp.lenBytes #= pktLen
        dmaWriteResp.sqpn #= dut.io.qpAttr.sqpn.toInt

        val respValid = true
        respValid
      }
    )

    streamSlaveAlwaysReady(dut.io.workCompPush, dut.clockDomain)
    onStreamFire(dut.io.workCompPush, dut.clockDomain) {
      outputQueue.enqueue(
        (
          dut.io.workCompPush.id.toBigInt,
          dut.io.workCompPush.opcode.toEnum,
          dut.io.workCompPush.status.toEnum,
          dut.io.workCompPush.lenBytes.toLong,
          dut.io.workCompPush.dqpn.toInt,
          dut.io.workCompPush.sqpn.toInt
        )
      )
    }

    MiscUtils.checkCondChangeOnceAndHoldAfterwards(
      dut.clockDomain,
      dut.io.cachedWorkReqAndAck.valid.toBoolean && dut.io.cachedWorkReqAndAck.ready.toBoolean,
      clue =
        f"${simTime()} time: dut.io.cachedWorkReqAndAck.fire=${dut.io.cachedWorkReqAndAck.valid.toBoolean && dut.io.cachedWorkReqAndAck.ready.toBoolean} should be true always"
    )
    MiscUtils.checkCondChangeOnceAndHoldAfterwards(
      dut.clockDomain,
      dut.io.workCompPush.valid.toBoolean,
      clue =
        f"${simTime()} time: dut.io.workCompPush.valid=${dut.io.workCompPush.valid.toBoolean} should be true always"
    )
    MiscUtils.checkExpectedOutputMatch(
      dut.clockDomain,
      inputQueue,
      outputQueue,
      MATCH_CNT
    )
  }

  test("WorkCompGen send/write WR case") {
    testFunc(AckType.NORMAL, needDmaWriteResp = false)
  }

  test("WorkCompGen read/atomic WR case") {
    testFunc(AckType.NORMAL, needDmaWriteResp = true)
  }

  test("WorkCompGen fatal error case") {
    testFunc(AckType.NAK_INV, needDmaWriteResp = false)
  }

  test("WorkCompGen retry limit exceed case") {
    testFunc(AckType.NAK_RNR, needDmaWriteResp = false)
  }
}

class RespHandlerTest extends AnyFunSuite {
  val busWidth = BusWidth.W512
  val pmtuLen = PMTU.U1024
  val maxFragNum = 37

  val simCfg = SimConfig.allOptimisation.withWave
    .withConfig(
      SpinalConfig(
        anonymSignalPrefix = "tmp",
        defaultClockDomainFrequency = FixedFrequency(200 MHz)
      )
    )
    .compile(new RespHandler(busWidth))

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

  test("RespHandler normal case") {
    testFunc(workReqAckReq = true, workCompGen = true)
  }

  def testFunc(workReqAckReq: Boolean, workCompGen: Boolean): Unit =
    simCfg.doSim { dut =>
      dut.clockDomain.forkStimulus(SIM_CYCLE_TIME)

      QpCtrlSim.connectRespHandler(
        dut.clockDomain,
        pmtuLen,
        dut.io.errNotifier,
        dut.io.retryNotifier,
        dut.io.qpAttr,
        dut.io.txQCtrl
      )

      // Input to DUT
      DmaWriteBusSim.reqStreamFixedDelayAndRespSuccess(
        dut.io.respDmaWrite,
        dut.clockDomain,
        fixedRespDelayCycles = DMA_WRITE_DELAY_CYCLES
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

      val inputWorkReqQueue = mutable.Queue[WorkReqData]()
      val inputRdmaRespPktFragQueue =
        mutable.Queue[(PSN, OpCode.Value, PadCnt, MTY, FragLast)]()
      val actualOutputQueue =
        mutable.Queue[
          (
              WorkReqId,
              SpinalEnumElement[WorkCompOpCode.type],
              PktLen,
              SpinalEnumElement[WorkCompStatus.type]
          )
        ]()
      val expectedOutputQueue =
        mutable.Queue[
          (
              WorkReqId,
              SpinalEnumElement[WorkCompOpCode.type],
              PktLen,
              SpinalEnumElement[WorkCompStatus.type]
          )
        ]()

      fork {
        val numWorkReqGen = 10

        while (true) {
          dut.clockDomain.waitSampling()

          val workReqGen = WorkReqSim.randomWorkReqGen(
            dut.io.cachedWorkReqPop.workReq,
            workReqAckReq,
            numWorkReqGen,
            maxFragNum,
            busWidth
          )
          inputWorkReqQueue.appendAll(workReqGen)

//        println(
//          f"${simTime()} time: inputWorkReqQueue.size=${inputWorkReqQueue.size} && expectedOutputQueue.size=${expectedOutputQueue.size}"
//        )
          dut.clockDomain.waitSamplingWhere(
            inputWorkReqQueue.isEmpty && expectedOutputQueue.isEmpty
          )

          println(f"${simTime()} time: normal WR process done")
        }
      }

//    var nextPsn = INIT_PSN
      streamMasterPayloadFromQueueNoRandomDelay(
        dut.io.cachedWorkReqPop,
        dut.clockDomain,
        inputWorkReqQueue,
        payloadAssignFunc = (
            cachedWorkReq: CachedWorkReq,
            payloadData: WorkReqData
        ) => {
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
          cachedWorkReq.workReq.id #= workReqId
          cachedWorkReq.workReq.opcode #= workReqOpCode
          cachedWorkReq.workReq.raddr #= remoteAddr
          cachedWorkReq.workReq.rkey #= rKey
          cachedWorkReq.workReq.ackReq #= ackReq
          if (workCompGen) {
            cachedWorkReq.workReq.flags.flagBits #= WorkReqSendFlagEnum.defaultEncoding
              .getValue(WorkReqSendFlagEnum.SIGNALED)
          } else {
            cachedWorkReq.workReq.flags.flagBits #= 0
          }
          cachedWorkReq.workReq.swap #= atomicSwap
          cachedWorkReq.workReq.comp #= atomicComp
          cachedWorkReq.workReq.immDtOrRmtKeyToInv #= immData
          cachedWorkReq.workReq.laddr #= localAddr
          cachedWorkReq.workReq.lenBytes #= pktLen
          cachedWorkReq.workReq.lkey #= lKey

          sleep(0)
          val (rdmaRespPktFragQueue, lastPsn, reqPktNum, respPktNum) =
            RdmaDataPktSim.rdmaRespPktFragGenFromWorkReq(
              cachedWorkReq.workReq,
              dut.io.qpAttr.npsn.toInt,
              pmtuLen,
              busWidth
            )
          cachedWorkReq.psnStart #= dut.io.qpAttr.npsn.toInt
          val pktNum = if (workReqOpCode.isReadReq()) {
            respPktNum
          } else {
            reqPktNum
          }
          cachedWorkReq.pktNum #= pktNum

          val respInputQueue = rdmaRespPktFragQueue.map { pktFragGenData =>
            val (psn, fragLast, _, _, _, padCnt, mty, _, opcode) =
              pktFragGenData
            (psn, opcode, padCnt, mty, fragLast)
          }
          inputRdmaRespPktFragQueue.appendAll(respInputQueue)

          val workCompOpCode = WorkCompSim.fromSqWorkReqOpCode(workReqOpCode)
          expectedOutputQueue.enqueue(
            (
              workReqId,
              workCompOpCode,
              pktLen,
              WorkCompStatus.SUCCESS
            )
          )

//          println(
//            f"${simTime()} time: psnStart=${dut.io.qpAttr.npsn.toInt}%X, workReqId=${workReqId}%X, workReqOpCode=${workReqOpCode}, workCompOpCode=${workCompOpCode}, pktLen=${pktLen}%X, reqPktNum=${reqPktNum}%X, respPktNum=${respPktNum}%X"
//          )
          dut.io.qpAttr.npsn #= lastPsn +% 1

          val reqValid = true
          reqValid
        }
      )
      onStreamFire(dut.io.cachedWorkReqPop, dut.clockDomain) {}

      streamMasterPayloadFromQueueNoRandomDelay(
        dut.io.rx.pktFrag,
        dut.clockDomain,
        inputRdmaRespPktFragQueue,
        payloadAssignFunc = (
            rdmaDataPkt: Fragment[RdmaDataPkt],
            payloadData: (PSN, OpCode.Value, PadCnt, MTY, FragLast)
        ) => {
          val (psn, opcode, padCnt, mty, fragLast) = payloadData
          rdmaDataPkt.bth.psn #= psn
          rdmaDataPkt.bth.opcodeFull #= opcode.id
          rdmaDataPkt.bth.padCnt #= padCnt
          rdmaDataPkt.mty #= mty
          rdmaDataPkt.last #= fragLast

          if (opcode.hasAeth()) {
            rdmaDataPkt.data #= AethSim.setNormalAck(
              rdmaDataPkt.data.toBigInt,
              busWidth
            )
          }

//        println(
//          f"${simTime()} time: PSN=${psn}, opcode=${opcode}, fragLast=${fragLast}"
//        )

          val reqValid = true
          reqValid
        }
      )

      streamSlaveAlwaysReady(dut.io.workComp, dut.clockDomain)
      onStreamFire(dut.io.workComp, dut.clockDomain) {
        val workCompID = dut.io.workComp.id.toBigInt
        val workCompOpCode = dut.io.workComp.opcode.toEnum
        val pktLen = dut.io.workComp.lenBytes.toLong
        val workCompStatus = dut.io.workComp.status.toEnum
        actualOutputQueue.enqueue(
          (
            workCompID,
            workCompOpCode,
            pktLen,
            workCompStatus
          )
        )

//        println(
//          f"${simTime()} time: WC output, workCompID=${workCompID}%X, workCompOpCode=${workCompOpCode}, pktLen=${pktLen}%X, workCompStatus=${workCompStatus}"
//        )
      }

      MiscUtils.checkExpectedOutputMatch(
        dut.clockDomain,
        expectedOutputQueue,
        actualOutputQueue,
        MATCH_CNT
      )
    }
}
