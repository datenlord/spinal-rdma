package rdma

import spinal.core._
import spinal.lib._

import BusWidth.BusWidth
import RdmaConstants._
import ConstantSettings._

// There is no alignment requirement for the source or
// destination buffers of a SEND message.
//
// pp. 291 spec 1.4
// A responder shall execute SEND requests, RDMA WRITE requests and ATOMIC
// Operation requests in the message order in which they are received.
// If the request is for an unsupported function or service,
// the appropriate response (for example, a NAK message, silent discard, or
// logging of the error) shall also be generated in the PSN order in which it
// was received.

// INCONSISTENT: atomic might not access memory in PSN order w.r.t. send and write

// There is no alignment requirement for the source or
// destination buffers of an RDMA READ message.

// RQ executes Send, Write, Atomic in order;
// RQ can delay Read execution;
// Completion of Send and Write at RQ is in PSN order,
// but not imply previous Read is complete unless fenced;
// RQ saves Atomic (Req & Result) and Read (Req only) in
// Queue Context, size as # pending Read/Atomic;
// TODO: RQ should send explicit ACK to SQ if it received many un-signaled requests
class RecvQ(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val psnInc = out(RqPsnInc())
    val notifier = out(RqNotifier())
    val rxQCtrl = in(RxQCtrl())
    val rxWorkReq = slave(Stream(RxWorkReq()))
    val addrCacheRead = master(QpAddrCacheAgentReadBus())
    val dma = master(RqDmaBus(busWidth))
    val rx = slave(RdmaDataBus(busWidth))
    val tx = master(RdmaDataBus(busWidth))
    val sendWriteWorkComp = master(Stream(WorkComp()))
  }

  val readAtomicRstCache =
    new ReadAtomicRstCache(MAX_PENDING_READ_ATOMIC_REQ_NUM)
  readAtomicRstCache.io.flush := io.rxQCtrl.stateErrFlush

  val reqCommCheck = new ReqCommCheck(busWidth)
  reqCommCheck.io.qpAttr := io.qpAttr
  reqCommCheck.io.rxQCtrl := io.rxQCtrl
  reqCommCheck.io.readAtomicRstCacheOccupancy := readAtomicRstCache.io.occupancy
  reqCommCheck.io.rx << io.rx
  io.psnInc.epsn := reqCommCheck.io.epsnInc
  io.notifier.clearRnrOrNakSeq := reqCommCheck.io.clearRnrOrNakSeq

  val reqRnrCheck = new ReqRnrCheck(busWidth)
  reqRnrCheck.io.qpAttr := io.qpAttr
  reqRnrCheck.io.rxQCtrl := io.rxQCtrl
  reqRnrCheck.io.rxWorkReq << io.rxWorkReq
  reqRnrCheck.io.rx << reqCommCheck.io.tx

  val dupReqLogic = new Area {
    val dupReqHandlerAndReadAtomicRstCacheQuery =
      new DupReqHandlerAndReadAtomicRstCacheQuery(busWidth)
    dupReqHandlerAndReadAtomicRstCacheQuery.io.qpAttr := io.qpAttr
    dupReqHandlerAndReadAtomicRstCacheQuery.io.rxQCtrl := io.rxQCtrl
    dupReqHandlerAndReadAtomicRstCacheQuery.io.rx << reqRnrCheck.io.txDupReq
    readAtomicRstCache.io.queryPort4DupReq << dupReqHandlerAndReadAtomicRstCacheQuery.io.readAtomicRstCache

    val dupReadDmaReqBuilder = new DupReadDmaReqBuilder(busWidth)
    dupReadDmaReqBuilder.io.qpAttr := io.qpAttr
    dupReadDmaReqBuilder.io.rxQCtrl := io.rxQCtrl
    dupReadDmaReqBuilder.io.rxDupReadReqAndRstCacheData << dupReqHandlerAndReadAtomicRstCacheQuery.io.dupReadReqAndRstCacheData
  }
  /*
  val dupReqLogic = new Area {
    val dupSendWriteReqHandlerAndDupResultAtomicRstCacheQueryBuilder =
      new DupSendWriteReqHandlerAndDupReadAtomicRstCacheQueryBuilder(busWidth)
    dupSendWriteReqHandlerAndDupResultAtomicRstCacheQueryBuilder.io.qpAttr := io.qpAttr
    dupSendWriteReqHandlerAndDupResultAtomicRstCacheQueryBuilder.io.rxQCtrl := io.rxQCtrl
    dupSendWriteReqHandlerAndDupResultAtomicRstCacheQueryBuilder.io.rx << reqCommCheck.io.txDupReq
    readAtomicRstCache.io.queryPort4DupReq.req << dupSendWriteReqHandlerAndDupResultAtomicRstCacheQueryBuilder.io.readAtomicRstCacheReq.req

    val dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator =
      new DupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator
    dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator.io.qpAttr := io.qpAttr
    dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator.io.rxQCtrl := io.rxQCtrl
    dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator.io.readAtomicRstCacheResp.resp << readAtomicRstCache.io.queryPort4DupReq.resp
    io.dma.dupRead.req << dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator.io.dmaReadReq.req

    val dupRqReadDmaRespHandler = new RqReadDmaRespHandler(busWidth)
//    dupRqReadDmaRespHandler.io.qpAttr := io.qpAttr
    dupRqReadDmaRespHandler.io.rxQCtrl := io.rxQCtrl
    dupRqReadDmaRespHandler.io.dmaReadResp.resp << io.dma.dupRead.resp
    dupRqReadDmaRespHandler.io.readRstCacheData << dupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator.io.readRstCacheData
//    readAtomicRstCache.io.queryPort4DupReqDmaRead << dupRqReadDmaRespHandler.io.readAtomicRstCacheQuery

    val dupReadRespSegment = new ReadRespSegment(busWidth)
    dupReadRespSegment.io.qpAttr := io.qpAttr
    dupReadRespSegment.io.rxQCtrl := io.rxQCtrl
    dupReadRespSegment.io.readRstCacheDataAndDmaReadResp << dupRqReadDmaRespHandler.io.readRstCacheDataAndDmaReadResp

    val dupReadRespGenerator = new ReadRespGenerator(busWidth)
    dupReadRespGenerator.io.qpAttr := io.qpAttr
    dupReadRespGenerator.io.rxQCtrl := io.rxQCtrl
    dupReadRespGenerator.io.readRstCacheDataAndDmaReadRespSegment << dupReadRespSegment.io.readRstCacheDataAndDmaReadRespSegment
  }
   */
  // TODO: make sure that when retry occurred, it only needs to flush reqValidateLogic
  // TODO: make sure that when error occurred, it needs to flush both reqCommCheck and reqValidateLogic
  val reqValidateLogic = new Area {
    val reqDmaInfoExtractor = new ReqDmaInfoExtractor(busWidth)
    reqDmaInfoExtractor.io.qpAttr := io.qpAttr
    reqDmaInfoExtractor.io.rxQCtrl := io.rxQCtrl
    reqDmaInfoExtractor.io.rx << reqRnrCheck.io.tx

    val reqAddrValidator = new ReqAddrValidator(busWidth)
    reqAddrValidator.io.qpAttr := io.qpAttr
    reqAddrValidator.io.rxQCtrl := io.rxQCtrl
    reqAddrValidator.io.rx << reqDmaInfoExtractor.io.tx
    io.addrCacheRead << reqAddrValidator.io.addrCacheRead

    val pktLenCheck = new PktLenCheck(busWidth)
    pktLenCheck.io.qpAttr := io.qpAttr
    pktLenCheck.io.rxQCtrl := io.rxQCtrl
    pktLenCheck.io.rx << reqAddrValidator.io.tx

    val reqSplitterAndNakGen = new ReqSplitterAndNakGen(busWidth)
    reqSplitterAndNakGen.io.qpAttr := io.qpAttr
    reqSplitterAndNakGen.io.rxQCtrl := io.rxQCtrl
    reqSplitterAndNakGen.io.rx << pktLenCheck.io.tx
    io.notifier.nak := reqSplitterAndNakGen.io.nakNotifier
  }

  val dmaReqLogic = new Area {
    val rqSendWriteDmaReqInitiator = new RqSendWriteDmaReqInitiator(busWidth)
    rqSendWriteDmaReqInitiator.io.qpAttr := io.qpAttr
    rqSendWriteDmaReqInitiator.io.rxQCtrl := io.rxQCtrl
    rqSendWriteDmaReqInitiator.io.rx << reqValidateLogic.reqSplitterAndNakGen.io.txSendWrite
    io.dma.sendWrite.req << rqSendWriteDmaReqInitiator.io.sendWriteDmaReq.req

    val rqReadAtomicDmaReqBuilder = new RqReadAtomicDmaReqBuilder(busWidth)
    rqReadAtomicDmaReqBuilder.io.qpAttr := io.qpAttr
    rqReadAtomicDmaReqBuilder.io.rxQCtrl := io.rxQCtrl
    rqReadAtomicDmaReqBuilder.io.rx << reqValidateLogic.reqSplitterAndNakGen.io.txReadAtomic
    readAtomicRstCache.io.push << rqReadAtomicDmaReqBuilder.io.readAtomicRstCachePush

    val readDmaReqInitiator = new ReadDmaReqInitiator
    readDmaReqInitiator.io.rxQCtrl := io.rxQCtrl
    readDmaReqInitiator.io.readDmaReqAndRstCacheData << rqReadAtomicDmaReqBuilder.io.readDmaReqAndRstCacheData
    readDmaReqInitiator.io.dupReadDmaReqAndRstCacheData << dupReqLogic.dupReadDmaReqBuilder.io.dupReadDmaReqAndRstCacheData
    io.dma.read.req << readDmaReqInitiator.io.readDmaReq.req
//    io.dma.atomic.rd.req << readAtomicDmaReqInitiator.io.atomicDmaReq.req
  }

  val respGenLogic = new Area {
    val sendWriteRespGenerator = new SendWriteRespGenerator(busWidth)
    sendWriteRespGenerator.io.qpAttr := io.qpAttr
    sendWriteRespGenerator.io.rxQCtrl := io.rxQCtrl
    sendWriteRespGenerator.io.rx << dmaReqLogic.rqSendWriteDmaReqInitiator.io.txSendWrite

    val rqReadDmaRespHandler = new RqReadDmaRespHandler(busWidth)
//    rqReadDmaRespHandler.io.qpAttr := io.qpAttr
    rqReadDmaRespHandler.io.rxQCtrl := io.rxQCtrl
//    rqReadDmaRespHandler.io.rx << dmaReqLogic.readAtomicReqExtractor.io.tx
    rqReadDmaRespHandler.io.dmaReadResp.resp << io.dma.read.resp
    rqReadDmaRespHandler.io.readRstCacheData << dmaReqLogic.readDmaReqInitiator.io.readRstCacheData
//    readAtomicRstCache.io.queryPort4DmaReadResp << rqReadDmaRespHandler.io.readAtomicRstCacheQuery

    val readRespSegment = new ReadRespSegment(busWidth)
    readRespSegment.io.qpAttr := io.qpAttr
    readRespSegment.io.rxQCtrl := io.rxQCtrl
    readRespSegment.io.readRstCacheDataAndDmaReadResp << rqReadDmaRespHandler.io.readRstCacheDataAndDmaReadResp

    val readRespGenerator = new ReadRespGenerator(busWidth)
    readRespGenerator.io.qpAttr := io.qpAttr
    readRespGenerator.io.rxQCtrl := io.rxQCtrl
    readRespGenerator.io.readRstCacheDataAndDmaReadRespSegment << readRespSegment.io.readRstCacheDataAndDmaReadRespSegment

    val atomicRespGenerator = new AtomicRespGenerator(busWidth)
    atomicRespGenerator.io.qpAttr := io.qpAttr
    atomicRespGenerator.io.rxQCtrl := io.rxQCtrl
    atomicRespGenerator.io.atomicDmaReqAndRstCacheData << dmaReqLogic.rqReadAtomicDmaReqBuilder.io.atomicDmaReqAndRstCacheData
    io.dma.atomic << atomicRespGenerator.io.dma
  }

  val rqSendWriteWorkCompGenerator = new RqSendWriteWorkCompGenerator
  rqSendWriteWorkCompGenerator.io.qpAttr := io.qpAttr
  rqSendWriteWorkCompGenerator.io.rxQCtrl := io.rxQCtrl
  rqSendWriteWorkCompGenerator.io.dmaWriteResp.resp << io.dma.sendWrite.resp
  rqSendWriteWorkCompGenerator.io.sendWriteWorkCompNormal << respGenLogic.sendWriteRespGenerator.io.sendWriteWorkCompAndAck
  rqSendWriteWorkCompGenerator.io.sendWriteWorkCompErr << reqValidateLogic.reqSplitterAndNakGen.io.sendWriteWorkCompErrAndNak
  io.sendWriteWorkComp << rqSendWriteWorkCompGenerator.io.sendWriteWorkCompOut

  val rqOut = new RqOut(busWidth)
  rqOut.io.qpAttr := io.qpAttr
  rqOut.io.outPsnRangeFifoPush << reqValidateLogic.reqDmaInfoExtractor.io.rqOutPsnRangeFifoPush
  rqOut.io.rxSendWriteResp << respGenLogic.sendWriteRespGenerator.io.tx
  rqOut.io.rxReadResp << respGenLogic.readRespGenerator.io.txReadResp
  rqOut.io.rxAtomicResp << respGenLogic.atomicRespGenerator.io.tx
  rqOut.io.rxDupSendWriteResp << dupReqLogic.dupReqHandlerAndReadAtomicRstCacheQuery.io.txDupSendWriteResp
//  rqOut.io.rxDupReadResp << dupReqLogic.dupReadDmaReqBuilder.io.txReadResp
  rqOut.io.rxDupAtomicResp << dupReqLogic.dupReqHandlerAndReadAtomicRstCacheQuery.io.txDupAtomicResp
  rqOut.io.rxErrResp << reqValidateLogic.reqSplitterAndNakGen.io.txErrResp
  rqOut.io.readAtomicRstCachePop << readAtomicRstCache.io.pop
  io.psnInc.opsn := rqOut.io.opsnInc
  io.tx << rqOut.io.tx
}

// PSN == ePSN, otherwise NAK-Seq;
// OpCode sequence, otherwise NAK-Inv Req;
// OpCode functionality is supported, otherwise NAK-Inv Req;
// First/Middle packets have padCnt == 0, otherwise NAK-Inv Req;
// Queue Context has resource for Read/Atomic, otherwise NAK-Inv Req;
// RKey, virtual address, DMA length (or packet size) match MR range and access type, otherwise NAK-Rmt Acc:
// - for Write, the length check is per packet basis, based on LRH:PktLen field;
// - for Read, the length check is based on RETH:DMA Length field;
// - no RKey check for 0-sized Write/Read;
// Length check, otherwise NAK-Inv Req:
// - for Send, the length check is based on LRH:PktLen field;
// - First/Middle packet length == PMTU;
// - Only packet length 0 <= len <= PMTU;
// - Last packet length 1 <= len <= PMTU;
// - for Write, check received data size == DMALen at last packet;
// - for Write/Read, check 0 <= DMALen <= 2^31;
// RQ local error detected, NAK-Rmt Op;
class ReqCommCheck(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val epsnInc = out(EPsnInc())
//    val nakNotifier = out(NakNotifier())
    val clearRnrOrNakSeq = out(RnrNakSeqClear())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RdmaDataBus(busWidth))
    val tx = master(RqReqCommCheckRstBus(busWidth))
//    val txDupReq = master(RdmaDataBus(busWidth))
    val readAtomicRstCacheOccupancy = in(
      UInt(log2Up(MAX_PENDING_READ_ATOMIC_REQ_NUM + 1) bits)
    )
  }

  val checkStage = new Area {
    val inputValid = io.rx.pktFrag.valid
    val inputPktFrag = io.rx.pktFrag.fragment
    val isLastFrag = io.rx.pktFrag.last

    // PSN sequence check
    val isPsnCheckPass = Bool()
    // The duplicate request is PSN < ePSN
    val isDupReq = Bool()

    val ePsnCmpRst = PsnUtil.cmp(
      psnA = inputPktFrag.bth.psn,
      psnB = io.qpAttr.epsn,
      curPsn = io.qpAttr.epsn
    )
    // The pending request is oPSN < PSN < ePSN
    val isDupPendingReq = False

    val isLargerThanOPsn = PsnUtil.gt(
      psnA = inputPktFrag.bth.psn,
      psnB = io.qpAttr.rqOutPsn,
      curPsn = io.qpAttr.epsn
    )

    switch(ePsnCmpRst) {
      is(PsnCompResult.GREATER) {
        isPsnCheckPass := False
        isDupReq := False
      }
      is(PsnCompResult.LESSER) {
        isPsnCheckPass := inputValid
        isDupPendingReq := inputValid && isLargerThanOPsn
        isDupReq := inputValid && !isLargerThanOPsn
      }
      default { // PsnCompResult.EQUAL
        isPsnCheckPass := inputValid
        isDupReq := False
      }
    }
    // OpCode sequence check
    val isOpSeqCheckPass =
      OpCodeSeq.checkReqSeq(io.qpAttr.rqPreReqOpCode, inputPktFrag.bth.opcode)
    // Is valid request opcode?
    val isSupportedOpCode = OpCode.isValidCode(inputPktFrag.bth.opcode) &&
      Transports.isSupportedType(inputPktFrag.bth.transport)
    // Packet padding count check
    val isPadCntCheckPass = reqPadCountCheck(
      inputPktFrag.bth.opcode,
      inputPktFrag.bth.padCnt,
      inputPktFrag.mty,
      isLastFrag,
      busWidth
    )

    // Check for # of pending read/atomic
    val isReadOrAtomicReq = OpCode.isReadReqPkt(inputPktFrag.bth.opcode) ||
      OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)
    val isReadAtomicRstCacheFull = inputValid && isReadOrAtomicReq &&
      io.readAtomicRstCacheOccupancy >= io.qpAttr.maxPendingReadAtomicReqNum

    // TODO: should discard duplicate pending requests?
    val throwCond = isDupPendingReq
    when(isDupPendingReq) {
      report(
        message =
          L"${REPORT_TIME} time: duplicated pending request received with PSN=${inputPktFrag.bth.psn} and opcode=${inputPktFrag.bth.opcode}, RQ opsn=${io.qpAttr.rqOutPsn}, epsn=${io.qpAttr.epsn}, maybe it should increase SQ timeout threshold",
        severity = FAILURE
      )
    }
    val output =
      io.rx.pktFrag.throwWhen(throwCond).translateWith { // No flush this stage
        val result = Fragment(RqReqCheckInternalOutput(busWidth))
        result.pktFrag := inputPktFrag
        result.checkRst.isPsnCheckPass := isPsnCheckPass
        result.checkRst.isDupReq := isDupReq
        result.checkRst.isOpSeqCheckPass := isOpSeqCheckPass
        result.checkRst.isSupportedOpCode := isSupportedOpCode
        result.checkRst.isPadCntCheckPass := isPadCntCheckPass
        result.checkRst.isReadAtomicRstCacheFull := isReadAtomicRstCacheFull
        result.checkRst.epsn := io.qpAttr.epsn
        result.last := io.rx.pktFrag.last
        result
      }

    // Increase ePSN
    val isExpectedPkt = inputValid && isPsnCheckPass && !isDupReq
    io.epsnInc.inc := isExpectedPkt && output.lastFire // Only update ePSN when each request packet ended
    io.epsnInc.incVal := ePsnIncrease(inputPktFrag, io.qpAttr.pmtu, busWidth)
    io.epsnInc.preReqOpCode := inputPktFrag.bth.opcode

    // Clear RNR or NAK SEQ if any
    io.clearRnrOrNakSeq.pulse := (io.rxQCtrl.rnrFlush || io.rxQCtrl.nakSeqTrigger) && isExpectedPkt
  }

  val outputStage = new Area {
    val input = cloneOf(checkStage.output)
    input <-/< checkStage.output

    val isPsnCheckPass = input.checkRst.isPsnCheckPass
    val isDupReq = input.checkRst.isDupReq
    val isInvReq =
      input.checkRst.isOpSeqCheckPass || input.checkRst.isSupportedOpCode ||
        input.checkRst.isPadCntCheckPass || input.checkRst.isReadAtomicRstCacheFull

    val hasNak = Bool()
    val nakAeth = AETH().set(AckType.NORMAL)
//    val txSel = UInt(1 bits)
//    val (dupIdx, otherIdx) = (0, 1)
    when(isPsnCheckPass) {
//      txSel := otherIdx
      hasNak := False
      when(isDupReq) {
//        txSel := dupIdx
        nakAeth.set(AckType.NAK_SEQ)
      }
    } elsewhen (!isInvReq) {
      hasNak := True
      nakAeth.set(AckType.NAK_SEQ)
//      txSel := otherIdx
    } otherwise { // NAK_INV
//      txSel := otherIdx
      hasNak := True
      nakAeth.set(AckType.NAK_INV)
    }
    /*
    val twoStreams = StreamDemux(
      input.throwWhen(io.rxQCtrl.flush),
      select = txSel,
      portCount = 2
    )

    io.txDupReq.pktFrag <-/< twoStreams(dupIdx).translateWith {
      val result = cloneOf(io.txDupReq.pktFrag.payloadType)
      result.fragment := input.pktFrag
      result.last := input.last
      result
    }
     */
    io.tx.checkRst <-/< input.throwWhen(io.rxQCtrl.flush).translateWith {
      val result = cloneOf(io.tx.checkRst.payloadType)
      result.pktFrag := input.pktFrag
      result.preOpCode := io.qpAttr.rqPreReqOpCode
      result.hasNak := hasNak
      result.nakAeth := nakAeth
      result.last := input.last
      result
    }
  }
}

class ReqRnrCheck(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rxWorkReq = slave(Stream(RxWorkReq()))
    val rx = slave(RqReqCommCheckRstBus(busWidth))
    val tx = master(RqReqWithRxBufBus(busWidth))
    val txDupReq = master(RqReqCommCheckRstBus(busWidth))
  }

  val inputValid = io.rx.checkRst.valid
  val inputPktFrag = io.rx.checkRst.pktFrag
  val isLastFrag = io.rx.checkRst.isLast
  val inputHasNak = io.rx.checkRst.hasNak

  val isSendOrWriteImmReq =
    OpCode.isSendReqPkt(inputPktFrag.bth.opcode) ||
      OpCode.isWriteWithImmReqPkt(inputPktFrag.bth.opcode)
  val isLastOrOnlySendOrWriteImmReq =
    OpCode.isSendLastOrOnlyReqPkt(inputPktFrag.bth.opcode) ||
      OpCode.isWriteWithImmReqPkt(inputPktFrag.bth.opcode)

  // RNR check for send/write
  val needRxBuf = inputValid && isSendOrWriteImmReq && !inputHasNak
  io.rxWorkReq.ready := isLastOrOnlySendOrWriteImmReq && !inputHasNak && io.rx.checkRst.lastFire
  val rxBuf = io.rxWorkReq.payload
  val rxBufValid = needRxBuf && io.rxWorkReq.valid
  val hasRnrErr = needRxBuf && !io.rxWorkReq.valid
  when(io.rxWorkReq.fire) {
    assert(
      assertion = io.rx.checkRst.lastFire,
      message =
        L"${REPORT_TIME} time: io.rxWorkReq.fire=${io.rxWorkReq.fire} should fire at the same cycle as io.rx.checkRst.lastFire=${io.rx.checkRst.lastFire} when needRxBuf=${needRxBuf}",
      severity = FAILURE
    )
  }

  val rnrAeth = AETH().set(AckType.NAK_RNR, io.qpAttr.rnrTimeOut)
  val nakAeth = cloneOf(io.rx.checkRst.nakAeth)
  nakAeth := io.rx.checkRst.nakAeth
  when(!inputHasNak && hasRnrErr) {
    nakAeth := rnrAeth
  }

  val (otherReqStream, dupReqStream) = StreamDeMuxByOneCondition(
    io.rx.checkRst.throwWhen(io.rxQCtrl.flush),
    nakAeth.isSeqNak()
  )
  io.txDupReq.checkRst <-/< dupReqStream

  io.tx.reqWithRxBuf <-/< otherReqStream
    .translateWith {
      val result = cloneOf(io.tx.reqWithRxBuf.payloadType)
      result.pktFrag := inputPktFrag
      result.preOpCode := io.rx.checkRst.preOpCode
      result.hasNak := inputHasNak || hasRnrErr
      result.nakAeth := nakAeth
      result.rxBufValid := rxBufValid
      result.rxBuf := rxBuf
      result.last := isLastFrag
      result
    }
}

// If multiple duplicate requests received, also ACK in PSN order;
// RQ will return ACK with the latest PSN for duplicate Send/Write, but this will NAK the following duplicate Read/Atomic???
// No NAK for duplicate requests if error detected;
// Duplicate Read is not valid if not with its original PSN and DMA range;
// Duplicate request with earlier PSN might interrupt processing of new request or duplicate request with later PSN;
// RQ does not re-execute the interrupted request, SQ will retry it;
// Discard duplicate Atomic if not match original PSN (should not happen);
class DupReqHandlerAndReadAtomicRstCacheQuery(
    busWidth: BusWidth
) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readAtomicRstCache = master(ReadAtomicRstCacheQueryBus())
    val rx = slave(RqReqCommCheckRstBus(busWidth))
    val txDupSendWriteResp = master(Stream(Acknowledge()))
    val txDupAtomicResp = master(Stream(AtomicResp()))
    val dupReadReqAndRstCacheData = master(
      Stream(RqDupReadReqAndRstCacheData(busWidth))
    )
  }

  val hasNak = io.rx.checkRst.hasNak
  val nakAeth = io.rx.checkRst.nakAeth
  val isLastFrag = io.rx.checkRst.isLast

  when(io.rx.checkRst.valid) {
    assert(
      assertion = hasNak && nakAeth.isSeqNak(),
      message =
        L"duplicate request should have hasNak=${hasNak} is true, and nakAeth with code=${nakAeth.code} and value=${nakAeth.value} is NAK SEQ",
      severity = FAILURE
    )
  }

  val (forkInputForSendWrite, forkInputForReadAtomic) = StreamFork2(
    io.rx.checkRst.throwWhen(io.rxQCtrl.stateErrFlush)
  )

  val isSendReq = OpCode.isSendReqPkt(forkInputForSendWrite.pktFrag.bth.opcode)
  val isWriteReq =
    OpCode.isWriteReqPkt(forkInputForSendWrite.pktFrag.bth.opcode)
  io.txDupSendWriteResp <-/< forkInputForSendWrite
    .takeWhen(isLastFrag && (isSendReq || isWriteReq))
    .translateWith(
      Acknowledge().setAck(
        AckType.NORMAL,
        io.qpAttr.epsn, // TODO: verify the ePSN is confirmed, will not retry later
        io.qpAttr.dqpn
      )
    )

  val buildReadAtomicRstCacheQuery =
    (pktFragStream: Stream[Fragment[RqReqCheckStageOutput]]) =>
      new Composite(pktFragStream, "buildReadAtomicRstCacheQuery") {
        val readAtomicRstCacheReq = ReadAtomicRstCacheReq()
        readAtomicRstCacheReq.psn := pktFragStream.pktFrag.bth.psn
      }.readAtomicRstCacheReq

  // Always expect response from ReadAtomicRstCache
  val expectReadAtomicRstCacheResp =
    (_: Stream[Fragment[RqReqCheckStageOutput]]) => True

  // Only send out ReadAtomicRstCache query when the input is
  // duplicate request
  val readAtomicRstCacheQueryCond =
    (pktFragStream: Stream[Fragment[RqReqCheckStageOutput]]) =>
      new Composite(pktFragStream, "readAtomicRstCacheQueryCond") {
        val result = pktFragStream.hasNak && pktFragStream.nakAeth.isSeqNak()
      }.result

  // Only join ReadAtomicRstCache query response with valid read/atomic request
  val readAtomicRstCacheQueryRespJoinCond =
    (pktFragStream: Stream[Fragment[RqReqCheckStageOutput]]) =>
      new Composite(pktFragStream, "readAtomicRstCacheQueryRespJoinCond") {
        val result = pktFragStream.valid
      }.result

  val joinStream = FragmentStreamForkQueryJoinResp(
    forkInputForReadAtomic.throwWhen(isSendReq || isWriteReq),
    io.readAtomicRstCache.req,
    io.readAtomicRstCache.resp,
    waitQueueDepth = READ_ATOMIC_RESULT_CACHE_QUERY_DELAY_CYCLE,
    buildQuery = buildReadAtomicRstCacheQuery,
    queryCond = readAtomicRstCacheQueryCond,
    expectResp = expectReadAtomicRstCacheResp,
    joinRespCond = readAtomicRstCacheQueryRespJoinCond
  )

  val readAtomicRstCacheRespValid = joinStream.valid
  val readAtomicRstCacheRespData = joinStream._2.cachedData
  val readAtomicRstCacheRespFound = joinStream._2.found
  val readAtomicResultNotFound =
    readAtomicRstCacheRespValid && !readAtomicRstCacheRespFound

  val isAtomicReq = OpCode.isAtomicReqPkt(readAtomicRstCacheRespData.opcode)
  val isReadReq = OpCode.isReadReqPkt(readAtomicRstCacheRespData.opcode)
  when(readAtomicRstCacheRespValid) {
    // Duplicate requests of pending requests are already discarded by ReqCommCheck
    assert(
      assertion = readAtomicRstCacheRespFound,
      message =
        L"${REPORT_TIME} time: duplicated read/atomic request with PSN=${joinStream._2.query.psn} not found, readAtomicRstCacheRespValid=${readAtomicRstCacheRespValid}, but readAtomicRstCacheRespFound=${readAtomicRstCacheRespFound}",
      severity = ERROR
    )
//    assert(
//      assertion = readAtomicRequestNotDone,
//      message =
//        L"${REPORT_TIME} time: duplicated read/atomic request with PSN=${joinStream._2.query.psn} not done yet, readAtomicRstCacheRespValid=${readAtomicRstCacheRespValid}, readAtomicRstCacheRespFound=${readAtomicRstCacheRespFound}, but readAtomicRstCacheRespData=${readAtomicRstCacheRespData.done}",
//      severity = FAILURE
//    )
  }
  val (forkJoinStream4Atomic, forkJoinStream4Read) = StreamFork2(
    joinStream.throwWhen(readAtomicResultNotFound)
  )
  // TODO: check duplicate atomic request is identical to the original one
  io.txDupAtomicResp <-/< forkJoinStream4Atomic
    .takeWhen(isAtomicReq)
    .translateWith {
      val result = cloneOf(io.txDupAtomicResp.payloadType)
      result.set(
        dqpn = io.qpAttr.dqpn,
        psn = readAtomicRstCacheRespData.psnStart,
        orig = readAtomicRstCacheRespData.atomicRst
      )
      result
    }

  io.dupReadReqAndRstCacheData <-/< forkJoinStream4Read
    .takeWhen(isReadReq)
    .translateWith {
      val result = cloneOf(io.dupReadReqAndRstCacheData.payloadType)
      result.pktFrag := forkJoinStream4Read._1.pktFrag
      result.cachedData := forkJoinStream4Read._2.cachedData
      when(isReadReq) {
        result.cachedData.duplicate := True
      }
      result
    }
}

class DupReadDmaReqBuilder(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rxDupReadReqAndRstCacheData = slave(
      Stream(RqDupReadReqAndRstCacheData(busWidth))
    )
//    val dupReadRstCacheData = master(Stream(ReadAtomicRstCacheData()))
//    val dmaReadReq = master(DmaReadReqBus())
    val dupReadDmaReqAndRstCacheData = master(
      Stream(RqDmaReadReqAndRstCacheData())
    )
  }

  val inputValid = io.rxDupReadReqAndRstCacheData.valid
  val inputPktFrag = io.rxDupReadReqAndRstCacheData.pktFrag
  val inputRstCacheData = io.rxDupReadReqAndRstCacheData.cachedData

  val (
    retryFromBeginning,
    retryStartPsn,
    retryDmaReadStartAddr,
    retryStartLocalAddr,
    retryDmaReadLenBytes
  ) = PartialRetry.readReqRetry(
    io.qpAttr,
    inputPktFrag,
    inputRstCacheData,
    inputValid
  )

//  val (dupReadReq4DmaReq, dupReadReq4RstCacheData) = StreamFork2(
//    io.rxDupReadReqAndRstCacheData.throwWhen(io.rxQCtrl.stateErrFlush)
//  )
//  io.dmaReadReq.req <-/< dupReadReq4DmaReq
//    .translateWith {
//      val result = cloneOf(io.dmaReadReq.req.payloadType)
//      result.set(
//        initiator = DmaInitiator.RQ_DUP,
//        sqpn = io.qpAttr.sqpn,
//        psnStart = retryStartPsn,
//        addr = retryDmaReadStartAddr,
//        lenBytes = retryDmaReadLenBytes.resize(RDMA_MAX_LEN_WIDTH)
//      )
//    }
//  io.dupReadRstCacheData <-/< dupReadReq4RstCacheData.translateWith {
//    dupReadReq4RstCacheData.cachedData
//  }

  io.dupReadDmaReqAndRstCacheData <-/< io.rxDupReadReqAndRstCacheData
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.dupReadDmaReqAndRstCacheData.payloadType)
      result.dmaReadReq.set(
        initiator = DmaInitiator.RQ_DUP,
        sqpn = io.qpAttr.sqpn,
        psnStart = retryStartPsn,
        pa = retryDmaReadStartAddr,
        lenBytes = retryDmaReadLenBytes // .resize(RDMA_MAX_LEN_WIDTH)
      )
      result.cachedData := inputRstCacheData
      result
    }

  when(inputValid) {
    assert(
      assertion = inputRstCacheData.duplicate,
      message =
        L"inputRstCacheData.duplicate=${inputRstCacheData.duplicate} should be true",
      severity = FAILURE
    )
  }
}
/*
class DupSendWriteReqHandlerAndDupReadAtomicRstCacheQueryBuilder(
    busWidth: BusWidth
) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readAtomicRstCacheReq = master(ReadAtomicRstCacheReqBus())
    val rx = slave(RdmaDataBus(busWidth))
    val txDupSendWriteResp = master(Stream(Acknowledge()))
//    val txDupAtomicResp = master(Stream(AtomicResp()))
//    val dmaReadReq = master(DmaReadReqBus())
  }

  val dupSendWriteRespHandlerAndReadAtomicRstCacheQueryBuilder = new Area {
    val inputPktFrag = io.rx.pktFrag.fragment

    val rdmaAck = Acknowledge()
      .setAck(
        AckType.NORMAL,
        io.qpAttr.epsn, // TODO: verify the ePSN is confirmed, will not retry later
        io.qpAttr.dqpn
      )

    val (sendWriteReqIdx, readAtomicReqIdx, otherReqIdx) = (0, 1, 2)
    val txSel = UInt(2 bits)
    when(
      OpCode.isSendReqPkt(inputPktFrag.bth.opcode) ||
        OpCode.isWriteReqPkt(inputPktFrag.bth.opcode)
    ) {
      txSel := sendWriteReqIdx
    } elsewhen (OpCode.isReadReqPkt(inputPktFrag.bth.opcode) ||
      OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)) {
      txSel := readAtomicReqIdx
    } otherwise {
      txSel := otherReqIdx
      report(
        message =
          L"${REPORT_TIME} time: invalid opcode=${inputPktFrag.bth.opcode}, should be send/write/read/atomic requests",
        severity = FAILURE
      )
    }
    val threeStreams = StreamDemux(
      // For duplicate requests, it's not affected by RNR NAK and NAK SEQ
      io.rx.pktFrag.throwWhen(io.rxQCtrl.stateErrFlush),
      select = txSel,
      portCount = 3
    )
    // Just discard non-send/write/read/atomic requests
    StreamSink(NoData) << threeStreams(otherReqIdx).translateWith(NoData)
    io.txDupSendWriteResp <-/< threeStreams(sendWriteReqIdx).translateWith(
      rdmaAck
    )
    io.readAtomicRstCacheReq.req <-/< threeStreams(readAtomicReqIdx)
      .translateWith {
        val result = ReadAtomicRstCacheReq()
        result.psn := inputPktFrag.bth.psn
        result
      }
  }
}

class DupReadAtomicRstCacheRespHandlerAndDupReadDmaInitiator extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readAtomicRstCacheResp = slave(ReadAtomicRstCacheRespBus())
    val readRstCacheData = master(Stream(ReadAtomicRstCacheData()))
    val txDupAtomicResp = master(Stream(AtomicResp()))
    val dmaReadReq = master(DmaReadReqBus())
  }

  val readAtomicRstCacheRespValid =
    io.readAtomicRstCacheResp.resp.valid
  val readAtomicRstCacheRespData =
    io.readAtomicRstCacheResp.resp.cachedData
  val readAtomicRstCacheRespFound =
    io.readAtomicRstCacheResp.resp.found

  val (readReqRstCacheIdx, atomicReqRstCacheIdx, otherReqRstCacheIdx) =
    (0, 1, 2)
  val txSel = UInt(2 bits)
  when(OpCode.isReadReqPkt(readAtomicRstCacheRespData.opcode)) {
    txSel := readReqRstCacheIdx
  } elsewhen (OpCode
    .isAtomicReqPkt(readAtomicRstCacheRespData.opcode)) {
    txSel := atomicReqRstCacheIdx
  } otherwise {
    txSel := otherReqRstCacheIdx
    report(
      message =
        L"${REPORT_TIME} time: invalid opcode=${readAtomicRstCacheRespData.opcode}, should be read/atomic requests",
      severity = FAILURE
    )
  }
  val threeStreams = StreamDemux(
    io.readAtomicRstCacheResp.resp.throwWhen(io.rxQCtrl.stateErrFlush),
    select = txSel,
    portCount = 3
  )
  // TODO: to remove this extra logic
  StreamSink(NoData) << threeStreams(otherReqRstCacheIdx).translateWith(NoData)

  //  val atomicResultThrowCond =
  //    readAtomicResultNotFound || readAtomicRequestNotDone
  val readAtomicResultNotFound =
    readAtomicRstCacheRespValid && !readAtomicRstCacheRespFound
  io.txDupAtomicResp <-/< threeStreams(atomicReqRstCacheIdx)
    .throwWhen(io.rxQCtrl.stateErrFlush || readAtomicResultNotFound)
    .translateWith {
      val result = cloneOf(io.txDupAtomicResp.payloadType)
      result.set(
        dqpn = io.qpAttr.dqpn,
        psn = readAtomicRstCacheRespData.psnStart,
        orig = readAtomicRstCacheRespData.atomicRst
      )
      result
    }
  when(readAtomicRstCacheRespValid) {
    // Duplicate requests of pending requests are already discarded by ReqCommCheck
//    val readAtomicRequestNotDone =
//      readAtomicRstCacheRespValid && readAtomicRstCacheRespValid && !readAtomicRstCacheRespData.done

    assert(
      assertion = readAtomicRstCacheRespFound,
      message =
        L"${REPORT_TIME} time: duplicated atomic request with PSN=${io.readAtomicRstCacheResp.resp.query.psn} not found, readAtomicRstCacheRespValid=${readAtomicRstCacheRespValid}, but readAtomicRstCacheRespFound=${readAtomicRstCacheRespFound}",
      severity = FAILURE
    )
//    assert(
//      assertion = readAtomicRequestNotDone,
//      message =
//        L"${REPORT_TIME} time: duplicated atomic request with PSN=${io.readAtomicRstCacheResp.resp.query.psn} not done yet, readAtomicRstCacheRespValid=${readAtomicRstCacheRespValid}, readAtomicRstCacheRespFound=${readAtomicRstCacheRespFound}, but readAtomicRstCacheRespData=${readAtomicRstCacheRespData.done}",
//      severity = FAILURE
//    )
  }

  val retryFromFirstReadResp =
    io.readAtomicRstCacheResp.resp.query.psn === readAtomicRstCacheRespData.psnStart
  // For partial read retry, compute the partial read DMA length
  val psnDiff = PsnUtil.diff(
    io.readAtomicRstCacheResp.resp.query.psn,
    readAtomicRstCacheRespData.psnStart
  )
  // psnDiff << io.qpAttr.pmtu.asUInt === psnDiff * pmtuPktLenBytes(io.qpAttr.pmtu)
  val dmaReadLenBytes =
    readAtomicRstCacheRespData.dlen - (psnDiff << io.qpAttr.pmtu.asUInt)
  when(!retryFromFirstReadResp) {
    assert(
      assertion =
        io.readAtomicRstCacheResp.resp.query.psn > readAtomicRstCacheRespData.psnStart,
      message =
        L"${REPORT_TIME} time: io.readAtomicRstCacheResp.resp.query.psn=${io.readAtomicRstCacheResp.resp.query.psn} should > readAtomicRstCacheRespData.psnStart=${readAtomicRstCacheRespData.psnStart}",
      severity = FAILURE
    )

    assert(
      assertion = psnDiff < computePktNum(
        readAtomicRstCacheRespData.dlen,
        io.qpAttr.pmtu
      ),
      message =
        L"${REPORT_TIME} time: psnDiff=${psnDiff} should < packet num=${computePktNum(readAtomicRstCacheRespData.dlen, io.qpAttr.pmtu)}",
      severity = FAILURE
    )
  }

  val (readRstCacheData4DmaReq, readRstCacheData4Output) = StreamFork2(
    threeStreams(readReqRstCacheIdx)
  )
  io.dmaReadReq.req <-/< readRstCacheData4DmaReq
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.dmaReadReq.req.payloadType)
      result.set(
        initiator = DmaInitiator.RQ_DUP,
        sqpn = io.qpAttr.sqpn,
        psnStart = io.readAtomicRstCacheResp.resp.query.psn,
        addr = readAtomicRstCacheRespData.pa,
        lenBytes = dmaReadLenBytes.resize(RDMA_MAX_LEN_WIDTH)
      )
    }
  io.readRstCacheData <-/< readRstCacheData4Output.translateWith(
    readRstCacheData4Output.cachedData
  )
}
 */
class ReqDmaInfoExtractor(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufBus(busWidth))
    val tx = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val rqOutPsnRangeFifoPush = master(Stream(RespPsnRange()))
  }

  val inputValid = io.rx.reqWithRxBuf.valid
  val inputPktFrag = io.rx.reqWithRxBuf.fragment
  val inputHasNak = io.rx.reqWithRxBuf.hasNak
  val isLastFrag = io.rx.reqWithRxBuf.last
  val isFirstFrag = io.rx.reqWithRxBuf.isFirst
  val rxBuf = io.rx.reqWithRxBuf.rxBuf

  val isSendOrWriteReq =
    OpCode.isSendFirstOrOnlyReqPkt(inputPktFrag.pktFrag.bth.opcode) ||
      OpCode.isWriteWithImmReqPkt(inputPktFrag.pktFrag.bth.opcode)
  val isReadReq = OpCode.isReadReqPkt(inputPktFrag.pktFrag.bth.opcode)
  val isAtomicReq = OpCode.isAtomicReqPkt(inputPktFrag.pktFrag.bth.opcode)
  // TODO: verify inputPktFrag.data is big endian
  val dmaInfo = DmaInfo().init()
  when(OpCode.isSendFirstOrOnlyReqPkt(inputPktFrag.pktFrag.bth.opcode)) {
    // Extract DMA info for send requests
    dmaInfo.va := rxBuf.laddr
    dmaInfo.lrkey := rxBuf.lkey
    dmaInfo.dlen := rxBuf.lenBytes

    when(inputValid) {
      assert(
        assertion = inputHasNak =/= io.rx.reqWithRxBuf.rxBufValid,
        message =
          L"${REPORT_TIME} time: inputHasNak=${inputHasNak} should != io.rx.reqWithRxBuf.rxBufValid=${io.rx.reqWithRxBuf.rxBufValid}",
        severity = FAILURE
      )
    }
  } elsewhen (
    OpCode.isWriteFirstOrOnlyReqPkt(inputPktFrag.pktFrag.bth.opcode) ||
      isReadReq || isAtomicReq
  ) {
    // Extract DMA info for write/read/atomic requests
    // TODO: verify inputPktFrag.data is big endian
    val rethBits = inputPktFrag.pktFrag.data(
      (busWidth.id - widthOf(BTH()) - widthOf(RETH())) until
        (busWidth.id - widthOf(BTH()))
    )
    val reth = RETH()
    reth.assignFromBits(rethBits)

    dmaInfo.va := reth.va
    dmaInfo.lrkey := reth.rkey
    dmaInfo.dlen := reth.dlen
    when(isAtomicReq) {
      dmaInfo.dlen := ATOMIC_DATA_LEN
    }
  }

  // Since the max length of a request is 2^31, and the min PMTU is 256=2^8
  // So the max number of packet is 2^23 < 2^PSN_WIDTH
  val numRespPkt =
    divideByPmtuUp(dmaInfo.dlen, io.qpAttr.pmtu).resize(PSN_WIDTH)

  val (txNormal, rqOutPsnRangeFifoPush) = StreamFork2(
    // Not flush when RNR
    io.rx.reqWithRxBuf.throwWhen(io.rxQCtrl.stateErrFlush)
  )
  io.tx.reqWithRxBufAndDmaInfo <-/< txNormal
    .translateWith {
      val result = cloneOf(io.tx.reqWithRxBufAndDmaInfo.payloadType)
      result.pktFrag := txNormal.pktFrag
      result.preOpCode := txNormal.preOpCode
      result.hasNak := txNormal.hasNak
      result.nakAeth := txNormal.nakAeth
      result.reqTotalLenValid := False
      result.reqTotalLenBytes := 0
      result.rxBufValid := txNormal.rxBufValid
      result.rxBuf := rxBuf
      result.dmaInfo := dmaInfo
      result.last := txNormal.last
      result
    }

  // Update output FIFO to keep output PSN order
  io.rqOutPsnRangeFifoPush <-/< rqOutPsnRangeFifoPush
    .takeWhen(
      inputHasNak || (isSendOrWriteReq && inputPktFrag.pktFrag.bth.ackreq) || isReadReq || isAtomicReq
    )
    .translateWith {
      val result = cloneOf(io.rqOutPsnRangeFifoPush.payloadType)
      result.opcode := inputPktFrag.pktFrag.bth.opcode
      result.start := inputPktFrag.pktFrag.bth.psn
      result.end := inputPktFrag.pktFrag.bth.psn
      when(isReadReq) {
        result.end := inputPktFrag.pktFrag.bth.psn + numRespPkt - 1
      }
      result
    }
}

class ReqAddrValidator(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val addrCacheRead = master(QpAddrCacheAgentReadBus())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val tx = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
  }

  // Build QpAddrCacheAgentReadReq according to RqReqWithRxBufAndDmaInfo
  val buildAddrCacheQuery =
    (pktFragStream: Stream[Fragment[RqReqWithRxBufAndDmaInfo]]) =>
      new Composite(pktFragStream, "buildAddrCacheQuery") {
        val inputValid = pktFragStream.valid
        val inputPktFrag = pktFragStream.pktFrag
        val inputHeader = pktFragStream.dmaInfo

        val accessKey = inputHeader.lrkey
        val va = inputHeader.va

        val accessType = AccessType() // Bits(ACCESS_TYPE_WIDTH bits)
        val pdId = io.qpAttr.pdId
        val remoteOrLocalKey = True // True: remote, False: local
        val dataLenBytes = inputHeader.dlen
        // Only send
        when(OpCode.isSendReqPkt(inputPktFrag.bth.opcode)) {
          accessType := AccessType.LOCAL_WRITE
        } elsewhen (OpCode.isWriteReqPkt(inputPktFrag.bth.opcode)) {
          accessType := AccessType.REMOTE_WRITE
        } elsewhen (OpCode.isReadReqPkt(inputPktFrag.bth.opcode)) {
          accessType := AccessType.REMOTE_READ
        } elsewhen (OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)) {
          accessType := AccessType.REMOTE_ATOMIC
        } otherwise {
          accessType := AccessType.LOCAL_READ // AccessType LOCAL_READ is not defined in spec. 1.4
          when(inputValid) {
            report(
              message =
                L"${REPORT_TIME} time: invalid opcode=${inputPktFrag.bth.opcode}, should be send/write/read/atomic",
              severity = FAILURE
            )
          }
        }

        val addrCacheReadReq = QpAddrCacheAgentReadReq()
        addrCacheReadReq.sqpn := io.qpAttr.sqpn
        addrCacheReadReq.psn := inputPktFrag.bth.psn
        addrCacheReadReq.key := accessKey
        addrCacheReadReq.pdId := pdId
        addrCacheReadReq.setKeyTypeRemoteOrLocal(isRemoteKey = remoteOrLocalKey)
        addrCacheReadReq.accessType := accessType
        addrCacheReadReq.va := va
        addrCacheReadReq.dataLenBytes := dataLenBytes
      }.addrCacheReadReq

  // Only expect AddrCache query response when the input has no NAK
  val expectAddrCacheResp =
    (pktFragStream: Stream[Fragment[RqReqWithRxBufAndDmaInfo]]) =>
      new Composite(pktFragStream, "expectAddrCacheResp") {
        val result = pktFragStream.valid && !pktFragStream.hasNak
      }.result

  // Only send out AddrCache query when the input data is the first fragment of
  // send/write/read/atomic request
  val addrCacheQueryCond =
    (pktFragStream: Stream[Fragment[RqReqWithRxBufAndDmaInfo]]) =>
      new Composite(pktFragStream, "addrCacheQueryCond") {
        val result = !pktFragStream.hasNak && pktFragStream.isFirst &&
          OpCode.isFirstOrOnlyReqPkt(pktFragStream.pktFrag.bth.opcode)
      }.result

  // Only join AddrCache query response when the input data is the last fragment of
  // the only or last send/write/read/atomic request
  val addrCacheQueryRespJoinCond =
    (pktFragStream: Stream[Fragment[RqReqWithRxBufAndDmaInfo]]) =>
      new Composite(pktFragStream, "addrCacheQueryRespJoinCond") {
        val result = pktFragStream.isLast &&
          OpCode.isLastOrOnlyReqPkt(pktFragStream.pktFrag.bth.opcode)
      }.result

  val joinStream = FragmentStreamForkQueryJoinResp(
    io.rx.reqWithRxBufAndDmaInfo.throwWhen(io.rxQCtrl.stateErrFlush),
    io.addrCacheRead.req,
    io.addrCacheRead.resp,
    waitQueueDepth = ADDR_CACHE_QUERY_DELAY_CYCLE,
    buildQuery = buildAddrCacheQuery,
    queryCond = addrCacheQueryCond,
    expectResp = expectAddrCacheResp,
    joinRespCond = addrCacheQueryRespJoinCond
  )

  val inputValid = joinStream.valid
  val inputPktFrag = joinStream._1.pktFrag
  val inputHasNak = joinStream._1.hasNak

  val addrCacheQueryResp = joinStream._2
  val sizeValid = addrCacheQueryResp.sizeValid
  val keyValid = addrCacheQueryResp.keyValid
  val accessValid = addrCacheQueryResp.accessValid
  val checkAddrCacheRespMatchCond =
    joinStream.isFirst && !joinStream._1.hasNak && OpCode
      .isFirstOrOnlyReqPkt(inputPktFrag.bth.opcode)
  when(inputValid && checkAddrCacheRespMatchCond) {
    assert(
      assertion = inputPktFrag.bth.psn === addrCacheQueryResp.psn,
      message =
        L"${REPORT_TIME} time: addrCacheReadResp.resp has PSN=${addrCacheQueryResp.psn} not match RQ query PSN=${inputPktFrag.bth.psn}",
      severity = FAILURE
    )
  }

  val bufLenErr = inputValid && !inputHasNak && !sizeValid
  val keyErr = inputValid && !inputHasNak && !keyValid
  val accessErr = inputValid && !inputHasNak && !accessValid
  val checkPass =
    inputValid && !inputHasNak && !keyErr && !bufLenErr && !accessErr

  val nakInvOrRmtAccAeth = AETH().setDefaultVal()
  when(bufLenErr) {
    nakInvOrRmtAccAeth.set(AckType.NAK_INV)
  } elsewhen (keyErr || accessErr) {
    nakInvOrRmtAccAeth.set(AckType.NAK_RMT_ACC)
  }

  val outputHasNak = !checkPass || inputHasNak
  val outputNakAeth = cloneOf(joinStream._1.nakAeth)
  outputNakAeth := joinStream._1.nakAeth
  when(!checkPass && !inputHasNak) {
    // TODO: if rdmaAck is retry NAK, should it be replaced by this NAK_INV or NAK_RMT_ACC?
    outputNakAeth := nakInvOrRmtAccAeth
  }

  io.tx.reqWithRxBufAndDmaInfo <-/< joinStream
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.tx.reqWithRxBufAndDmaInfo.payloadType)
      result.pktFrag := joinStream._1.pktFrag
      result.preOpCode := joinStream._1.preOpCode
      result.hasNak := outputHasNak
      result.nakAeth := outputNakAeth
      result.reqTotalLenValid := False
      result.reqTotalLenBytes := 0
      result.rxBufValid := joinStream._1.rxBufValid
      result.rxBuf := joinStream._1.rxBuf
      result.dmaInfo.pa := joinStream._2.pa
      result.dmaInfo.lrkey := joinStream._1.dmaInfo.lrkey
      result.dmaInfo.va := joinStream._1.dmaInfo.va
      result.dmaInfo.dlen := joinStream._1.dmaInfo.dlen
      result.last := joinStream.isLast
      result
    }
}

// TODO: verify ICRC has been stripped off
class PktLenCheck(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val tx = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
  }

  val inputHasNak = io.rx.reqWithRxBufAndDmaInfo.hasNak
  val inputValid = io.rx.reqWithRxBufAndDmaInfo.valid
  val inputFire = io.rx.reqWithRxBufAndDmaInfo.fire
  val inputPktFrag = io.rx.reqWithRxBufAndDmaInfo.pktFrag
  val inputRxBuf = io.rx.reqWithRxBufAndDmaInfo.rxBuf
  val inputDmaInfo = io.rx.reqWithRxBufAndDmaInfo.dmaInfo
  val isLastFrag = io.rx.reqWithRxBufAndDmaInfo.isLast
  val isFirstFrag = io.rx.reqWithRxBufAndDmaInfo.isFirst

  val pktFragLenBytes =
    CountOne(inputPktFrag.mty).resize(RDMA_MAX_LEN_WIDTH)
  when(inputValid) {
    assert(
      assertion = inputPktFrag.mty =/= 0,
      message =
        L"invalid MTY=${inputPktFrag.mty}, opcode=${inputPktFrag.bth.opcode}, PSN=${inputPktFrag.bth.psn}, isFirstFrag=${isFirstFrag}, isLastFrag=${isLastFrag}",
      severity = FAILURE
    )
  }
  val dmaTargetLenBytes = io.rx.reqWithRxBufAndDmaInfo.dmaInfo.dlen
  val bthLenBytes = widthOf(BTH()) / BYTE_WIDTH
  val rethLenBytes = widthOf(RETH()) / BYTE_WIDTH
  val immDtLenBytes = widthOf(ImmDt()) / BYTE_WIDTH
  val iethLenBytes = widthOf(IETH()) / BYTE_WIDTH

  when(inputValid && io.rx.reqWithRxBufAndDmaInfo.rxBufValid) {
    assert(
      assertion = OpCode.isSendReqPkt(inputPktFrag.bth.opcode) ||
        OpCode.isWriteWithImmReqPkt(inputPktFrag.bth.opcode),
      message =
        L"${REPORT_TIME} time: it should be send requests that require receive buffer, but opcode=${inputPktFrag.bth.opcode}",
      severity = FAILURE
    )
  }

  val padCntAdjust = inputPktFrag.bth.padCnt
  val rethLenAdjust = U(0, log2Up(rethLenBytes) + 1 bits)
  when(OpCode.isWriteFirstOrOnlyReqPkt(inputPktFrag.bth.opcode)) {
    rethLenAdjust := rethLenBytes
  }
  val immDtLenAdjust = U(0, log2Up(immDtLenBytes) + 1 bits)
  when(OpCode.hasImmDt(inputPktFrag.bth.opcode)) {
    immDtLenAdjust := immDtLenBytes
  }
  val iethLenAdjust = U(0, log2Up(iethLenBytes) + 1 bits)
  when(OpCode.hasIeth(inputPktFrag.bth.opcode)) {
    iethLenAdjust := iethLenBytes
  }

  val pktLenBytesReg = RegInit(U(0, RDMA_MAX_LEN_WIDTH bits))
  val reqTotalLenBytesReg = RegInit(U(0, RDMA_MAX_LEN_WIDTH bits))
  when(io.rxQCtrl.stateErrFlush) {
    pktLenBytesReg := 0
    reqTotalLenBytesReg := 0
  }

  // All the packet length adjustments used when isFirstFrag
  // NOTE: use +^ to avoid addition overflow
  val firstPktAdjust = bthLenBytes +^ rethLenAdjust
  val midPktAdjust = U(bthLenBytes)
  val lastPktEthAdjust = padCntAdjust +^ immDtLenAdjust +^ iethLenAdjust
  val lastPktAdjust = bthLenBytes +^ lastPktEthAdjust
  val onlyPktAdjust = firstPktAdjust +^ lastPktEthAdjust

  val curPktFragLenBytesFirstPktAdjust = pktFragLenBytes - firstPktAdjust
  val curPktFragLenBytesMidPktAdjust = pktFragLenBytes - midPktAdjust
  val curPktFragLenBytesLastPktAdjust = pktFragLenBytes - lastPktAdjust
  val curPktFragLenBytesOnlyPktAdjust = pktFragLenBytes - onlyPktAdjust

  val isSendReq = OpCode.isSendReqPkt(inputPktFrag.bth.opcode)
  val isWriteReq = OpCode.isWriteReqPkt(inputPktFrag.bth.opcode)
  // TODO: verify reqTotalLenValid and reqTotalLenBytes have correct value and timing
  val curPktLenBytes = pktLenBytesReg + pktFragLenBytes
  val curReqTotalLenBytes = reqTotalLenBytesReg + curPktLenBytes
  // Used when last request packet and isFirstFrag && isLastFrag
  val curReqTotalLenBytesLastPktAdjust =
    reqTotalLenBytesReg + curPktFragLenBytesLastPktAdjust

  val isReqTotalLenCheckErr = False
  val isPktLenCheckErr = False
  val reqTotalLenValid = False
  val reqTotalLenBytes = U(0, RDMA_MAX_LEN_WIDTH bits)
  when(inputFire && (isSendReq || isWriteReq)) {
    switch(isFirstFrag ## isLastFrag) {
      is(True ## False) { // isFirstFrag && !isLastFrag // First fragment should consider RDMA header adjustment
        when(OpCode.isFirstReqPkt(inputPktFrag.bth.opcode)) {
          pktLenBytesReg := curPktFragLenBytesFirstPktAdjust
        } elsewhen (OpCode.isLastReqPkt(inputPktFrag.bth.opcode)) {
          pktLenBytesReg := curPktFragLenBytesLastPktAdjust
        } elsewhen (OpCode.isOnlyReqPkt(inputPktFrag.bth.opcode)) {
          pktLenBytesReg := curPktFragLenBytesOnlyPktAdjust
        } elsewhen (OpCode.isMidReqPkt(inputPktFrag.bth.opcode)) {
          pktLenBytesReg := curPktFragLenBytesMidPktAdjust
        } otherwise {
          report(
            message =
              L"${REPORT_TIME} time: illegal opcode=${inputPktFrag.bth.opcode} when isFirstFrag=${isFirstFrag} and isLastFrag=${isLastFrag}",
            severity = FAILURE
          )
        }
      }
      is(False ## True) { // !isFirstFrag && isLastFrag // Last packet
        pktLenBytesReg := 0

        when(OpCode.isLastOrOnlyReqPkt(inputPktFrag.bth.opcode)) {
          reqTotalLenBytesReg := 0
          reqTotalLenValid := True
          reqTotalLenBytes := curReqTotalLenBytes
        } elsewhen (OpCode.isFirstReqPkt(inputPktFrag.bth.opcode)) {
          reqTotalLenBytesReg := curReqTotalLenBytes

          isPktLenCheckErr :=
            curPktLenBytes =/= pmtuPktLenBytes(io.qpAttr.pmtu) ||
              padCntAdjust =/= 0
          assert(
            assertion = !isPktLenCheckErr,
            message =
              L"${REPORT_TIME} time: first request packet length check failed, opcode=${inputPktFrag.bth.opcode}, PSN=${inputPktFrag.bth.psn}, curPktLenBytes=${curPktLenBytes}, padCnt=${padCntAdjust}",
            severity = NOTE
          )
        } elsewhen (OpCode.isMidReqPkt(inputPktFrag.bth.opcode)) {
          reqTotalLenBytesReg := curReqTotalLenBytes

          isPktLenCheckErr :=
            curPktLenBytes =/= pmtuPktLenBytes(io.qpAttr.pmtu) ||
              padCntAdjust =/= 0
          assert(
            assertion = !isPktLenCheckErr,
            message =
              L"${REPORT_TIME} time: middle request packet length check failed, opcode=${inputPktFrag.bth.opcode}, PSN=${inputPktFrag.bth.psn}, curPktLenBytes=${curPktLenBytes}, padCnt=${padCntAdjust}",
            severity = NOTE
          )
        }
      }
      is(True ## True) { // isFirstFrag && isLastFrag // Only one fragment
        pktLenBytesReg := 0

        when(OpCode.isOnlyReqPkt(inputPktFrag.bth.opcode)) {
          reqTotalLenBytesReg := 0
          reqTotalLenValid := True
          reqTotalLenBytes := curPktFragLenBytesOnlyPktAdjust
        } elsewhen (OpCode.isLastReqPkt(inputPktFrag.bth.opcode)) {
          reqTotalLenBytesReg := 0
          reqTotalLenValid := True
          reqTotalLenBytes := curReqTotalLenBytesLastPktAdjust
        } elsewhen (OpCode.isFirstOrMidReqPkt(inputPktFrag.bth.opcode)) {
          report(
            message =
              L"${REPORT_TIME} time: first or middle request packet length check failed, opcode=${inputPktFrag.bth.opcode}, when isFirstFrag=${isFirstFrag} and isLastFrag=${isLastFrag}",
            severity = FAILURE // TODO: change to NOTE
          )

          isPktLenCheckErr := True
        }
      }
      is(False ## False) { // !isFirstFrag && !isLastFrag // Middle fragment
        pktLenBytesReg := curPktLenBytes
      }
    }
  }

  when(reqTotalLenValid) {
    isReqTotalLenCheckErr :=
      (isSendReq && reqTotalLenBytes > dmaTargetLenBytes) ||
        (isWriteReq && reqTotalLenBytes =/= dmaTargetLenBytes)
    assert(
      assertion = !isReqTotalLenCheckErr,
      message =
        L"${REPORT_TIME} time: request total length check failed, opcode=${inputPktFrag.bth.opcode}, PSN=${inputPktFrag.bth.psn}, dmaTargetLenBytes=${dmaTargetLenBytes}, reqTotalLenBytes=${reqTotalLenBytes} = (reqTotalLenBytesReg=${reqTotalLenBytesReg} + pktLenBytesReg=${pktLenBytesReg} + pktFragLenBytes=${pktFragLenBytes}), inputPktFrag.mty=${inputPktFrag.mty}, CountOne(inputPktFrag.mty)=${CountOne(
          inputPktFrag.mty
        )}, bthLenBytes=${U(bthLenBytes)}, rethLenAdjust=${rethLenAdjust}, immDtLenAdjust=${immDtLenAdjust}, iethLenAdjust=${iethLenAdjust}, padCntAdjust=${padCntAdjust}, isFirstFrag=${isFirstFrag}, isLastFrag=${isLastFrag}",
      severity = NOTE
    )
  }

  val hasLenCheckErr = isReqTotalLenCheckErr || isPktLenCheckErr
  val nakInvAeth = AETH().set(AckType.NAK_INV)
  val nakAeth = cloneOf(io.rx.reqWithRxBufAndDmaInfo.nakAeth)
  val outputHasNak = inputHasNak || hasLenCheckErr
  nakAeth := io.rx.reqWithRxBufAndDmaInfo.nakAeth
  when(hasLenCheckErr && !inputHasNak) {
    // TODO: if rdmaAck is retry NAK, should it be replaced by this NAK_INV or NAK_RMT_ACC?
    nakAeth := nakInvAeth
  }

  io.tx.reqWithRxBufAndDmaInfo <-/< io.rx.reqWithRxBufAndDmaInfo
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.rx.reqWithRxBufAndDmaInfo.payloadType)
      result.pktFrag := inputPktFrag
      result.preOpCode := io.rx.reqWithRxBufAndDmaInfo.preOpCode
      result.hasNak := outputHasNak
      result.nakAeth := nakAeth
      result.rxBufValid := io.rx.reqWithRxBufAndDmaInfo.rxBufValid
      result.rxBuf := inputRxBuf
      result.reqTotalLenValid := reqTotalLenValid
      result.reqTotalLenBytes := reqTotalLenBytes
      result.dmaInfo := inputDmaInfo
      result.last := isLastFrag
      result
    }
}

class ReqSplitterAndNakGen(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val txSendWrite = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val txReadAtomic = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val nakNotifier = out(RqNakNotifier())
    val txErrResp = master(Stream(Acknowledge()))
    val sendWriteWorkCompErrAndNak = master(Stream(WorkCompAndAck()))
  }

  val inputPktFrag = io.rx.reqWithRxBufAndDmaInfo.pktFrag
  val inputHasNak = io.rx.reqWithRxBufAndDmaInfo.hasNak
  val isSendReq = OpCode.isSendReqPkt(inputPktFrag.bth.opcode)
  val isWriteReq = OpCode.isWriteReqPkt(inputPktFrag.bth.opcode)
  val isReadReq = OpCode.isReadReqPkt(inputPktFrag.bth.opcode)
  val isAtomicReq = OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)

  val (errReqIdx, sendWriteIdx, readAtomicIdx) = (0, 1, 2)
  val threeStreams = StreamDeMuxByConditions(
    io.rx.reqWithRxBufAndDmaInfo.throwWhen(io.rxQCtrl.stateErrFlush),
    inputHasNak, // Error request
    !inputHasNak && (isSendReq || isWriteReq), // Normal send/write request
    !inputHasNak && (isReadReq || isAtomicReq) // Normal read/atomic request
  )
  io.txSendWrite.reqWithRxBufAndDmaInfo <-/< threeStreams(sendWriteIdx)
  io.txReadAtomic.reqWithRxBufAndDmaInfo <-/< threeStreams(readAtomicIdx)

  // NAK generation
  val nakResp = Acknowledge().setAck(
    io.rx.reqWithRxBufAndDmaInfo.nakAeth,
    inputPktFrag.bth.psn,
    io.qpAttr.dqpn
  )
  val isRetryNak = nakResp.aeth.isRetryNak()
  val errReqStream = threeStreams(errReqIdx)
  val (errResp4Tx, errResp4WorkComp) = StreamFork2(errReqStream)
  io.txErrResp <-/< errResp4Tx
    // TODO: better to response at the first fragment when error
    .takeWhen(errResp4Tx.isLast)
    .translateWith(nakResp)
  io.sendWriteWorkCompErrAndNak <-/< errResp4WorkComp
    .throwWhen(io.rxQCtrl.stateErrFlush || isRetryNak)
    // TODO: better to response at the first fragment when error
    .takeWhen(errResp4WorkComp.rxBufValid && errResp4WorkComp.isLast)
    .translateWith {
      val result = cloneOf(io.sendWriteWorkCompErrAndNak.payloadType)
      result.workComp.setFromRxWorkReq(
        errResp4WorkComp.rxBuf,
        errResp4WorkComp.pktFrag.bth.opcode,
        io.qpAttr.dqpn,
        nakResp.aeth.toWorkCompStatus(),
        errResp4WorkComp.reqTotalLenBytes, // for error WC, reqTotalLenBytes does not matter here
        errResp4WorkComp.pktFrag.data
      )
      result.ackValid := errResp4WorkComp.valid
      result.ack := nakResp
      result
    }

  io.nakNotifier.setFromAeth(
    aeth = io.rx.reqWithRxBufAndDmaInfo.nakAeth,
    pulse = errReqStream.isLast,
    preOpCode = io.rx.reqWithRxBufAndDmaInfo.preOpCode,
    psn = inputPktFrag.bth.psn
  )
}

class RqSendWriteDmaReqInitiator(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
//    val txReadAtomic = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val txSendWrite = master(RqReqWithRxBufAndDmaInfoBus(busWidth))
//    val readDmaReq = master(DmaReadReqBus())
    val sendWriteDmaReq = master(DmaWriteReqBus(busWidth))
//    val nakNotifier = out(RqNakNotifier())
//    val txErrResp = master(Stream(Acknowledge()))
//    val sendWriteWorkCompErrAndNak = master(Stream(WorkCompAndAck()))
  }

  val inputHasNak = io.rx.reqWithRxBufAndDmaInfo.hasNak
  val inputValid = io.rx.reqWithRxBufAndDmaInfo.valid
  val inputPktFrag = io.rx.reqWithRxBufAndDmaInfo.pktFrag
  val isLastFrag = io.rx.reqWithRxBufAndDmaInfo.last
  val inputDmaHeader = io.rx.reqWithRxBufAndDmaInfo.dmaInfo
  val isEmptyReq = inputValid && inputDmaHeader.dlen === 0

  when(inputValid) {
    assert(
      assertion = !inputHasNak,
      message =
        L"inputHasNak=${inputHasNak} should be false in RqSendWriteDmaReqInitiator",
      severity = FAILURE
    )
  }
//  val isSendReq = OpCode.isSendReqPkt(inputPktFrag.bth.opcode)
//  val isWriteReq = OpCode.isWriteReqPkt(inputPktFrag.bth.opcode)
//  val isReadReq = OpCode.isReadReqPkt(inputPktFrag.bth.opcode)
//  val isAtomicReq = OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)

//  val (sendWriteIdx, errRespIdx) = (0, 1)
//  val (normalReqStream, errReqStream) = StreamDeMuxByOneCondition(
//    io.rx.reqWithRxBufAndDmaInfo.throwWhen(io.rxQCtrl.stateErrFlush),
//    inputHasNak
//  )

//  val readAtomicReqStream = threeStreams(readAtomicIdx)
//  val (forkReadAtomicReqStream4DmaRead, forkReadAtomicReqStream4Output) =
//    StreamFork2(readAtomicReqStream)
//
//  io.readDmaReq.req <-/< forkReadAtomicReqStream4DmaRead
//    .throwWhen(io.rxQCtrl.stateErrFlush || isEmptyReq)
//    .translateWith {
//      val result = cloneOf(io.readDmaReq.req.payloadType)
//      result.set(
//        initiator = DmaInitiator.RQ_RD,
//        sqpn = io.qpAttr.sqpn,
//        psnStart = inputPktFrag.bth.psn,
//        addr = inputDmaHeader.pa,
//        lenBytes = inputDmaHeader.dlen
//      )
//    }
//  io.txReadAtomic.reqWithRxBufAndDmaInfo <-/< forkReadAtomicReqStream4Output
//  io.txReadAtomic.reqWithRxBufAndDmaInfo <-/< threeStreams(readAtomicIdx)

//  val sendWriteReqStream = twoStream(sendWriteIdx)
  val (forkSendWriteReqStream4DmaWrite, forkSendWriteReqStream4Output) =
    StreamFork2(
      io.rx.reqWithRxBufAndDmaInfo.throwWhen(io.rxQCtrl.stateErrFlush)
    )
  io.sendWriteDmaReq.req <-/< forkSendWriteReqStream4DmaWrite
    .throwWhen(io.rxQCtrl.stateErrFlush || isEmptyReq)
    .translateWith {
      val result = cloneOf(io.sendWriteDmaReq.req.payloadType)
      result.last := isLastFrag
      result.set(
        initiator = DmaInitiator.RQ_WR,
        sqpn = io.qpAttr.sqpn,
        psn = inputPktFrag.bth.psn,
        pa = inputDmaHeader.pa,
        workReqId = io.rx.reqWithRxBufAndDmaInfo.rxBuf.id,
        data = inputPktFrag.data,
        mty = inputPktFrag.mty
      )
      result
    }
  io.txSendWrite.reqWithRxBufAndDmaInfo <-/< forkSendWriteReqStream4Output
  /*
  val nakResp = Acknowledge().setAck(
    io.rx.reqWithRxBufAndDmaInfo.nakAeth,
    inputPktFrag.bth.psn,
    io.qpAttr.dqpn
  )
  val isRetryNak = nakResp.aeth.isRetryNak()
  val (errResp4Tx, errResp4WorkComp) = StreamFork2(errReqStream)
  io.txErrResp <-/< errResp4Tx
    // TODO: better to response at the first fragment when error
    .takeWhen(errResp4Tx.isLast)
    .translateWith(nakResp)
  io.sendWriteWorkCompErrAndNak <-/< errResp4WorkComp
    .throwWhen(io.rxQCtrl.stateErrFlush || isRetryNak)
    // TODO: better to response at the first fragment when error
    .takeWhen(errResp4WorkComp.rxBufValid && errResp4WorkComp.isLast)
    .translateWith {
      val result = cloneOf(io.sendWriteWorkCompErrAndNak.payloadType)
      result.workComp.setFromRxWorkReq(
        errResp4WorkComp.rxBuf,
        errResp4WorkComp.pktFrag.bth.opcode,
        io.qpAttr.dqpn,
        nakResp.aeth.toWorkCompStatus(),
        errResp4WorkComp.reqTotalLenBytes, // for error WC, reqTotalLenBytes does not matter here
        errResp4WorkComp.pktFrag.data
      )
      result.ackValid := errResp4WorkComp.valid
      result.ack := nakResp
      result
    }
  io.nakNotifier.setFromAeth(
    aeth = io.rx.reqWithRxBufAndDmaInfo.nakAeth,
    pulse = errReqStream.isLast,
    preOpCode = io.rx.reqWithRxBufAndDmaInfo.preOpCode,
    psn = inputPktFrag.bth.psn
  )
   */
}

class RqReadAtomicDmaReqBuilder(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val readAtomicRstCachePush = master(Stream(ReadAtomicRstCacheData()))
    val readDmaReqAndRstCacheData = master(
      Stream(RqDmaReadReqAndRstCacheData())
    )
    val atomicDmaReqAndRstCacheData = master(
      Stream(RqDmaReadReqAndRstCacheData())
    )
  }

  val inputHasNak = io.rx.reqWithRxBufAndDmaInfo.hasNak
  val inputValid = io.rx.reqWithRxBufAndDmaInfo.valid
  val inputPktFrag = io.rx.reqWithRxBufAndDmaInfo.pktFrag
  val inputDmaHeader = io.rx.reqWithRxBufAndDmaInfo.dmaInfo
  val isLastFrag = io.rx.reqWithRxBufAndDmaInfo.last

  val isReadReq = OpCode.isReadReqPkt(inputPktFrag.bth.opcode)
  val isAtomicReq = OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)
  when(inputValid) {
    assert(
      assertion = isReadReq || isAtomicReq,
      message =
        L"${REPORT_TIME} time: ReadAtomicDmaReqBuilder can only handle read/atomic request, but opcode=${inputPktFrag.bth.opcode}",
      severity = FAILURE
    )
    assert(
      assertion = !inputHasNak,
      message =
        L"inputHasNak=${inputHasNak} should be false in RqReadAtomicDmaReqBuilder",
      severity = FAILURE
    )
  }

  val (reth, atomicEth) = extractReadAtomicEth(inputPktFrag, busWidth)
//  // BTH is included in inputPktFrag.data
//  // TODO: verify inputPktFrag.data is big endian
//  val rethBits = inputPktFrag.data(
//    (busWidth.id - widthOf(BTH()) - widthOf(RETH())) until
//      (busWidth.id - widthOf(BTH()))
//  )
//  val reth = RETH()
//  reth.assignFromBits(rethBits)
//
//  // BTH is included in inputPktFrag.data
//  // TODO: verify inputPktFrag.data is big endian
//  val atomicEthBits = inputPktFrag.data(
//    (busWidth.id - widthOf(BTH()) - widthOf(AtomicEth())) until
//      (busWidth.id - widthOf(BTH()))
//  )
//  val atomicEth = AtomicEth()
//  atomicEth.assignFromBits(atomicEthBits)

  val readAtomicDmaReqAndRstCacheData = io.rx.reqWithRxBufAndDmaInfo
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.readDmaReqAndRstCacheData.payloadType)
      when(isReadReq) {
        result.dmaReadReq.set(
          initiator = DmaInitiator.RQ_RD,
          sqpn = io.qpAttr.sqpn,
          psnStart = inputPktFrag.bth.psn,
          pa = inputDmaHeader.pa,
          lenBytes = inputDmaHeader.dlen
        )
      } otherwise {
        result.dmaReadReq.set(
          initiator = DmaInitiator.RQ_RD,
          sqpn = io.qpAttr.sqpn,
          psnStart = inputPktFrag.bth.psn,
          pa = inputDmaHeader.pa,
          lenBytes = ATOMIC_DATA_LEN
        )
      }
      val rstCacheData = result.cachedData
      rstCacheData.psnStart := inputPktFrag.bth.psn
      rstCacheData.pktNum := computePktNum(reth.dlen, io.qpAttr.pmtu)
      rstCacheData.opcode := inputPktFrag.bth.opcode
      rstCacheData.pa := io.rx.reqWithRxBufAndDmaInfo.dmaInfo.pa
      when(isReadReq) {
        rstCacheData.va := reth.va
        rstCacheData.rkey := reth.rkey
        rstCacheData.dlen := reth.dlen
        rstCacheData.swap := 0
        rstCacheData.comp := 0
      } otherwise {
        rstCacheData.va := atomicEth.va
        rstCacheData.rkey := atomicEth.rkey
        rstCacheData.dlen := ATOMIC_DATA_LEN
        rstCacheData.swap := atomicEth.swap
        rstCacheData.comp := atomicEth.comp
      }
      rstCacheData.atomicRst := 0
      rstCacheData.duplicate := False
      result
    }
  /*
    val readAtomicReqStream = threeStreams(readAtomicIdx)
    val (forkReadAtomicReqStream4DmaRead, forkReadAtomicReqStream4Output) =
      StreamFork2(readAtomicReqStream)

    io.readDmaReq.req <-/< forkReadAtomicReqStream4DmaRead
      .throwWhen(io.rxQCtrl.stateErrFlush || isEmptyReq)
      .translateWith {
        val result = cloneOf(io.readDmaReq.req.payloadType)
        result.set(
          initiator = DmaInitiator.RQ_RD,
          sqpn = io.qpAttr.sqpn,
          psnStart = inputPktFrag.bth.psn,
          addr = inputDmaHeader.pa,
          lenBytes = inputDmaHeader.dlen
        )
      }

  val readAtomicRstCacheData = io.rx.reqWithRxBufAndDmaInfo
    .throwWhen(io.rxQCtrl.stateErrFlush)
    .translateWith {
      val result = cloneOf(io.readAtomicRstCachePush.payloadType)
      result.psnStart := inputPktFrag.bth.psn
      result.pktNum := computePktNum(reth.dlen, io.qpAttr.pmtu)
      result.opcode := inputPktFrag.bth.opcode
      result.pa := io.rx.reqWithRxBufAndDmaInfo.dmaInfo.pa
      when(isReadReq) {
        result.va := reth.va
        result.rkey := reth.rkey
        result.dlen := reth.dlen
        result.swap := 0
        result.comp := 0
      } otherwise {
        result.va := atomicEth.va
        result.rkey := atomicEth.rkey
        result.dlen := ATOMIC_DATA_LEN
        result.swap := atomicEth.swap
        result.comp := atomicEth.comp
      }
      result.atomicRst := 0
      result.duplicate := False
      result
    }
   */
  val (rstCacheDataPush, readAtomicReqOutput) =
    StreamFork2(readAtomicDmaReqAndRstCacheData)
  io.readAtomicRstCachePush <-/< rstCacheDataPush.translateWith(
    rstCacheDataPush.cachedData
  )

  val (readIdx, atomicIdx) = (0, 1)
  val twoStreams =
    StreamDeMuxByConditions(readAtomicReqOutput, isReadReq, isAtomicReq)
//  val txSel = UInt(2 bits)
//  val (readIdx, atomicIdx, otherIdx) = (0, 1, 2)
//  when(isReadReq) {
//    txSel := readIdx
//  } elsewhen (isAtomicReq) {
//    txSel := atomicIdx
//  } otherwise {
//    report(
//      message =
//        L"${REPORT_TIME} time: invalid opcode=${inputPktFrag.bth.opcode}, should be read/atomic request",
//      severity = FAILURE
//    )
//    txSel := otherIdx
//  }
//  val threeStreams =
//    StreamDemux(
//      input = readAtomicRstCacheData4Output,
//      select = txSel,
//      portCount = 3
//    )
//  StreamSink(NoData) << threeStreams(otherIdx).translateWith(NoData)
  io.readDmaReqAndRstCacheData <-/< twoStreams(readIdx)
  io.atomicDmaReqAndRstCacheData <-/< twoStreams(atomicIdx)
}

class ReadDmaReqInitiator() extends Component {
  val io = new Bundle {
    val rxQCtrl = in(RxQCtrl())
    val readDmaReqAndRstCacheData = slave(Stream(RqDmaReadReqAndRstCacheData()))
    val dupReadDmaReqAndRstCacheData = slave(
      Stream(RqDmaReadReqAndRstCacheData())
    )
    val readDmaReq = master(DmaReadReqBus())
    val readRstCacheData = master(Stream(ReadAtomicRstCacheData()))
  }

  val readDmaReqArbitration = StreamArbiterFactory.roundRobin.transactionLock
    .onArgs(
      io.readDmaReqAndRstCacheData.throwWhen(io.rxQCtrl.stateErrFlush),
      io.dupReadDmaReqAndRstCacheData.throwWhen(io.rxQCtrl.stateErrFlush)
    )

  val inputValid = readDmaReqArbitration.valid
  val inputDmaHeader = readDmaReqArbitration.dmaReadReq
  val isEmptyReq = inputValid && inputDmaHeader.lenBytes === 0

  val (readDmaReq, readRstCacheData) = StreamFork2(readDmaReqArbitration)
  io.readDmaReq.req <-/< readDmaReq
    .throwWhen(isEmptyReq)
    .translateWith(readDmaReq.dmaReadReq)
  io.readRstCacheData <-/< readRstCacheData.translateWith(
    readRstCacheData.cachedData
  )
}

// TODO: prevent coalesce response to too many send/write requests
class SendWriteRespGenerator(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val rx = slave(RqReqWithRxBufAndDmaInfoBus(busWidth))
    val tx = master(Stream(Acknowledge()))
    val sendWriteWorkCompAndAck = master(Stream(WorkCompAndAck()))
  }

  val inputValid = io.rx.reqWithRxBufAndDmaInfo.valid
  val inputPktFrag = io.rx.reqWithRxBufAndDmaInfo.pktFrag
  val isFirstFrag = io.rx.reqWithRxBufAndDmaInfo.isFirst
  val isLastFrag = io.rx.reqWithRxBufAndDmaInfo.isLast
  val inputDmaHeader = io.rx.reqWithRxBufAndDmaInfo.dmaInfo
//  val isEmptyReq = io.rx.reqWithRxBufAndDmaInfo.dmaHeaderValid && inputDmaHeader.dlen === 0

  val isSendOrWriteImmReq =
    OpCode.isSendFirstOrOnlyReqPkt(inputPktFrag.bth.opcode) ||
      OpCode.isWriteWithImmReqPkt(inputPktFrag.bth.opcode)
  val inputShouldHaveRxWorkReq =
    inputValid && isFirstFrag && isSendOrWriteImmReq
  when(inputShouldHaveRxWorkReq) {
    assert(
      assertion = io.rx.reqWithRxBufAndDmaInfo.rxBufValid,
      message =
        L"${REPORT_TIME} time: both inputShouldHaveRxWorkReq=${inputShouldHaveRxWorkReq} and io.rx.reqWithRxBufAndDmaInfo.rxBufValid=${io.rx.reqWithRxBufAndDmaInfo.rxBufValid} should be true",
      severity = FAILURE
    )
  }

  when(io.rx.reqWithRxBufAndDmaInfo.valid) {
    assert(
      assertion = OpCode.isSendReqPkt(inputPktFrag.bth.opcode) ||
        OpCode.isWriteReqPkt(inputPktFrag.bth.opcode),
      message =
        L"${REPORT_TIME} time: invalid opcode=${inputPktFrag.bth.opcode}, should be send/write requests",
      severity = FAILURE
    )
  }

//  when(
//    OpCode.isSendFirstOrOnlyReqPkt(inputPktFrag.bth.opcode) ||
//      OpCode.isWriteFirstOrOnlyReqPkt(inputPktFrag.bth.opcode) ||
//      OpCode.isReadReqPkt(inputPktFrag.bth.opcode) ||
//      OpCode.isAtomicReqPkt(inputPktFrag.bth.opcode)
//  ) {
//    when(inputValid && isFirstFrag) {
//      assert(
//        assertion = io.rx.reqWithRxBufAndDmaInfo.dmaHeaderValid,
//        message =
//          L"${REPORT_TIME} time: io.rx.dmaHeaderValid=${io.rx.reqWithRxBufAndDmaInfo.dmaHeaderValid} should be true for the first fragment of a request",
//        severity = FAILURE
//      )
//    }
//  }

  val rdmaAck =
    Acknowledge().setAck(AckType.NORMAL, inputPktFrag.bth.psn, io.qpAttr.dqpn)
  val (req4Tx, req4WorkComp) = StreamFork2(
    io.rx.reqWithRxBufAndDmaInfo
      .throwWhen(io.rxQCtrl.stateErrFlush || !inputPktFrag.bth.ackreq)
  )

  // Responses to send/write do not wait for DMA write responses
  io.tx <-/< req4Tx.translateWith(rdmaAck)
  io.sendWriteWorkCompAndAck <-/< req4WorkComp
    .takeWhen(req4WorkComp.reqTotalLenValid)
    .translateWith {
      val result = cloneOf(io.sendWriteWorkCompAndAck.payloadType)
      result.workComp.setSuccessFromRxWorkReq(
        req4WorkComp.rxBuf,
        req4WorkComp.pktFrag.bth.opcode,
        io.qpAttr.dqpn,
        req4WorkComp.reqTotalLenBytes,
        req4WorkComp.pktFrag.data
      )
      result.ackValid := req4WorkComp.valid
      result.ack := rdmaAck
      result
    }
}

/** When received a new DMA response, combine the DMA response and
  * ReadAtomicRstCacheData to downstream, also handle
  * zero DMA length read request.
  */
class RqReadDmaRespHandler(busWidth: BusWidth) extends Component {
  val io = new Bundle {
//    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readRstCacheData = slave(Stream(ReadAtomicRstCacheData()))
    val dmaReadResp = slave(DmaReadRespBus(busWidth))
    val readRstCacheDataAndDmaReadResp = master(
      Stream(Fragment(ReadAtomicRstCacheDataAndDmaReadResp(busWidth)))
    )
  }

  val handlerOutput = DmaReadRespHandler(
    io.readRstCacheData,
    io.dmaReadResp,
    io.rxQCtrl.stateErrFlush,
    busWidth,
    isReqZeroDmaLen =
      (readRstCacheData: ReadAtomicRstCacheData) => readRstCacheData.dlen === 0
  )

  io.readRstCacheDataAndDmaReadResp <-/< handlerOutput.translateWith {
    val result = cloneOf(io.readRstCacheDataAndDmaReadResp.payloadType)
    result.dmaReadResp := handlerOutput.dmaReadResp
    result.resultCacheData := handlerOutput.req
    result.last := handlerOutput.last
    result
  }
}

class ReadRespSegment(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readRstCacheDataAndDmaReadResp = slave(
      Stream(Fragment(ReadAtomicRstCacheDataAndDmaReadResp(busWidth)))
    )
    val readRstCacheDataAndDmaReadRespSegment = master(
      Stream(Fragment(ReadAtomicRstCacheDataAndDmaReadResp(busWidth)))
    )
  }

  val segmentOut = DmaReadRespSegment(
    io.readRstCacheDataAndDmaReadResp,
    io.rxQCtrl.stateErrFlush,
    io.qpAttr.pmtu,
    busWidth,
    isReqZeroDmaLen =
      (reqAndDmaReadResp: ReadAtomicRstCacheDataAndDmaReadResp) =>
        reqAndDmaReadResp.resultCacheData.dlen === 0
  )
  io.readRstCacheDataAndDmaReadRespSegment <-/< segmentOut
}

class ReadRespGenerator(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val readRstCacheDataAndDmaReadRespSegment = slave(
      Stream(Fragment(ReadAtomicRstCacheDataAndDmaReadResp(busWidth)))
    )
    val txReadResp = master(RdmaDataBus(busWidth))
  }

  val busWidthBytes = busWidth.id / BYTE_WIDTH

  val input = io.readRstCacheDataAndDmaReadRespSegment
  when(input.valid) {
    assert(
      assertion = input.resultCacheData.dlen === input.dmaReadResp.lenBytes,
      message =
        L"${REPORT_TIME} time: input.resultCacheData.dlen=${input.resultCacheData.dlen} should equal input.dmaReadResp.lenBytes=${input.dmaReadResp.lenBytes}",
      severity = FAILURE
    )
  }

  val reqAndDmaReadRespSegment = input.translateWith {
    val result =
      Fragment(ReqAndDmaReadResp(ReadAtomicRstCacheData(), busWidth))
    result.dmaReadResp := input.dmaReadResp
    result.req := input.resultCacheData
    result.last := input.isLast
    result
  }

  val combinerOutput = CombineHeaderAndDmaResponse(
    reqAndDmaReadRespSegment,
    io.qpAttr,
    io.rxQCtrl.stateErrFlush,
    busWidth,
//    pktNumFunc = (req: ReadAtomicRstCacheData, _: QpAttrData) => req.pktNum,
    headerGenFunc = (
        inputRstCacheData: ReadAtomicRstCacheData,
        inputDmaDataFrag: DmaReadResp,
        curReadRespPktCntVal: UInt,
        qpAttr: QpAttrData
    ) =>
      new Composite(this) {
        val numReadRespPkt = inputRstCacheData.pktNum
        val lastOrOnlyReadRespPktLenBytes =
          moduloByPmtu(inputDmaDataFrag.lenBytes, qpAttr.pmtu)

        val isFromFirstResp =
          inputDmaDataFrag.psnStart === inputRstCacheData.psnStart
        val curPsn = inputDmaDataFrag.psnStart + curReadRespPktCntVal
        val opcode = Bits(OPCODE_WIDTH bits)
        val padCnt = U(0, PAD_COUNT_WIDTH bits)

        val bth = BTH().set(
          opcode = opcode,
          padCnt = padCnt,
          dqpn = qpAttr.dqpn,
          psn = curPsn
        )
        val aeth = AETH().set(AckType.NORMAL)
        val bthMty = Bits(widthOf(bth) / BYTE_WIDTH bits).setAll()
        val aethMty = Bits(widthOf(aeth) / BYTE_WIDTH bits).setAll()

        val headerBits = Bits(busWidth.id bits)
        val headerMtyBits = Bits(busWidthBytes bits)
        when(numReadRespPkt > 1) {
          when(curReadRespPktCntVal === 0) {
            when(isFromFirstResp) {
              opcode := OpCode.RDMA_READ_RESPONSE_FIRST.id

              // TODO: verify endian
              headerBits := (bth ## aeth).resize(busWidth.id)
              headerMtyBits := (bthMty ## aethMty).resize(busWidthBytes)
            } otherwise {
              opcode := OpCode.RDMA_READ_RESPONSE_MIDDLE.id

              // TODO: verify endian
              headerBits := bth.asBits.resize(busWidth.id)
              headerMtyBits := bthMty.resize(busWidthBytes)
            }
          } elsewhen (curReadRespPktCntVal === numReadRespPkt - 1) {
            opcode := OpCode.RDMA_READ_RESPONSE_LAST.id
            padCnt := (PAD_COUNT_FULL -
              lastOrOnlyReadRespPktLenBytes(0, PAD_COUNT_WIDTH bits))
              .resize(PAD_COUNT_WIDTH)

            // TODO: verify endian
            headerBits := (bth ## aeth).resize(busWidth.id)
            headerMtyBits := (bthMty ## aethMty).resize(busWidthBytes)
          } otherwise {
            opcode := OpCode.RDMA_READ_RESPONSE_MIDDLE.id

            // TODO: verify endian
            headerBits := bth.asBits.resize(busWidth.id)
            headerMtyBits := bthMty.resize(busWidthBytes)
          }
        } otherwise {
          when(isFromFirstResp) {
            opcode := OpCode.RDMA_READ_RESPONSE_ONLY.id
          } otherwise {
            opcode := OpCode.RDMA_READ_RESPONSE_LAST.id
          }
          padCnt := (PAD_COUNT_FULL -
            lastOrOnlyReadRespPktLenBytes(0, PAD_COUNT_WIDTH bits))
            .resize(PAD_COUNT_WIDTH)

          // TODO: verify endian
          headerBits := (bth ## aeth).resize(busWidth.id)
          headerMtyBits := (bthMty ## aethMty).resize(busWidthBytes)
        }

        val result = CombineHeaderAndDmaRespInternalRst(busWidth)
          .set(inputRstCacheData.pktNum, bth, headerBits, headerMtyBits)
      }.result
  )
  io.txReadResp.pktFrag <-/< combinerOutput.pktFrag
}

// pp. 286 spec 1.4
// If an RDMA READ work request is posted before an ATOMIC Operation
// work request then the atomic may execute its remote memory
// operations before the previous RDMA READ has read its data.
// This can occur because the responder is allowed to delay execution
// of the RDMA READ. Strict ordering can be assured by posting
// the ATOMIC Operation work request with the fence modifier.
//
// When a sequence of requests arrives at a QP, the ATOMIC Operation
// only accesses memory after prior (non-RDMA READ) requests
// access memory and before subsequent requests access memory.
// Since the responder takes time to issue the response to the atomic
// request, and this response takes more time to reach the requester
// and even more time for the requester to create a completion queue
// entry, requests after the atomic may access the responders memory
// before the requester writes the completion queue entry for the
// ATOMIC Operation request.
class AtomicRespGenerator(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val atomicDmaReqAndRstCacheData = slave(
      Stream(RqDmaReadReqAndRstCacheData())
    )
    val tx = master(Stream(AtomicResp()))
    val dma = master(DmaBus(busWidth))
  }

//  val (atomicDmaReq, atomicRstCacheData) = StreamFork2(io.atomicDmaReqAndRstCacheData.throwWhen(io.rxQCtrl.stateErrFlush))
//  io.atomicDmaReq.req <-/< atomicDmaReq.translateWith(atomicDmaReq.dmaReadReq)
//  io.atomicRstCacheData <-/< atomicRstCacheData.translateWith(atomicRstCacheData.cachedData)

  // TODO: support atomic request
  StreamSink(NoData) << io.atomicDmaReqAndRstCacheData.translateWith(NoData)
  val atomicResp = AtomicResp().setDefaultVal()

  io.dma.rd.req << io.dma.rd.resp.translateWith(
    DmaReadReq().set(
      initiator = DmaInitiator.RQ_ATOMIC_RD,
      psnStart = atomicResp.bth.psn,
      sqpn = io.qpAttr.sqpn,
      pa = 0,
      lenBytes = ATOMIC_DATA_LEN
    )
  )
  io.dma.wr.req << io.dma.wr.resp.translateWith {
    val result = Fragment(DmaWriteReq(busWidth))
    result.set(
      initiator = DmaInitiator.RQ_ATOMIC_WR,
      sqpn = io.qpAttr.sqpn,
      psn = atomicResp.bth.psn,
      pa = 0,
      workReqId = 0, // TODO: RQ has no WR ID, need refactor
      data = 0,
      mty = 0
    )
    result.last := True
    result
  }
  io.tx << StreamSource().translateWith(atomicResp)
}

class RqSendWriteWorkCompGenerator extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val dmaWriteResp = slave(DmaWriteRespBus())
    val sendWriteWorkCompNormal = slave(Stream(WorkCompAndAck()))
    val sendWriteWorkCompErr = slave(Stream(WorkCompAndAck()))
    val sendWriteWorkCompOut = master(Stream(WorkComp()))
  }

  val workCompAndAckQueuePop =
    io.sendWriteWorkCompNormal.queueLowLatency(DMA_WRITE_DELAY_CYCLE)

  val dmaWriteRespPnsLessThanWorkCompPsn = PsnUtil.lt(
    io.dmaWriteResp.resp.psn,
    workCompAndAckQueuePop.ack.bth.psn,
    io.qpAttr.epsn
  )
  val dmaWriteRespPnsEqualWorkCompPsn =
    io.dmaWriteResp.resp.psn === workCompAndAckQueuePop.ack.bth.psn
  io.dmaWriteResp.resp.ready := !workCompAndAckQueuePop.valid
  when(workCompAndAckQueuePop.valid) {
    when(dmaWriteRespPnsLessThanWorkCompPsn) {
      io.dmaWriteResp.resp.ready := io.dmaWriteResp.resp.valid
    } elsewhen (dmaWriteRespPnsEqualWorkCompPsn) {
      io.dmaWriteResp.resp.ready := io.dmaWriteResp.resp.valid && io.sendWriteWorkCompOut.fire
    }
  }
  val continueCond =
    io.dmaWriteResp.resp.valid && dmaWriteRespPnsEqualWorkCompPsn
  io.sendWriteWorkCompOut <-/< workCompAndAckQueuePop
    .continueWhen(continueCond)
    .translateWith(workCompAndAckQueuePop.workComp)

  // TODO: output io.sendWriteWorkCompErr
  StreamSink(NoData) << io.sendWriteWorkCompErr.translateWith(NoData)
}

// For duplicated requests, also return ACK in PSN order
// TODO: after RNR and NAK SEQ returned, no other nested NAK send
class RqOut(busWidth: BusWidth) extends Component {
  val io = new Bundle {
    val qpAttr = in(QpAttrData())
    val rxQCtrl = in(RxQCtrl())
    val opsnInc = out(OPsnInc())
    val outPsnRangeFifoPush = slave(Stream(RespPsnRange()))
    val readAtomicRstCachePop = slave(Stream(ReadAtomicRstCacheData()))
    val rxSendWriteResp = slave(Stream(Acknowledge()))
    val rxReadResp = slave(
      RdmaDataBus(busWidth)
    ) // Normal and duplicate read response
    val rxAtomicResp = slave(Stream(AtomicResp()))
    val rxErrResp = slave(Stream(Acknowledge()))
    val rxDupSendWriteResp = slave(Stream(Acknowledge()))
//    val rxDupReadResp = slave(RdmaDataBus(busWidth))
    val rxDupAtomicResp = slave(Stream(AtomicResp()))
    val tx = master(RdmaDataBus(busWidth))
  }

  // TODO: set max pending request number using QpAttrData
  val psnOutRangeFifo = StreamFifoLowLatency(
    io.outPsnRangeFifoPush.payloadType(),
    depth = PENDING_REQ_NUM
  )
  psnOutRangeFifo.io.push << io.outPsnRangeFifoPush

  val rxSendWriteResp = io.rxSendWriteResp.translateWith(
    io.rxSendWriteResp.toRdmaDataPktFrag(busWidth)
  )
  val rxAtomicResp =
    io.rxAtomicResp.translateWith(io.rxAtomicResp.toRdmaDataPktFrag(busWidth))
  val rxErrResp =
    io.rxErrResp.translateWith(io.rxErrResp.toRdmaDataPktFrag(busWidth))
  val rxDupSendWriteResp = io.rxDupSendWriteResp.translateWith(
    io.rxDupSendWriteResp.toRdmaDataPktFrag(busWidth)
  )
  val rxDupAtomicResp =
    io.rxDupAtomicResp.translateWith(
      io.rxDupAtomicResp.toRdmaDataPktFrag(busWidth)
    )
  val txVec =
    Vec(rxSendWriteResp, io.rxReadResp.pktFrag, rxAtomicResp, rxErrResp)
  val txSelOH = txVec.map(resp => {
    val psnRangeMatch =
      psnOutRangeFifo.io.pop.start <= resp.bth.psn && resp.bth.psn <= psnOutRangeFifo.io.pop.end
    when(psnRangeMatch && psnOutRangeFifo.io.pop.valid) {
      assert(
        assertion =
          checkRespOpCodeMatch(psnOutRangeFifo.io.pop.opcode, resp.bth.opcode),
        message =
          L"${REPORT_TIME} time: request opcode=${psnOutRangeFifo.io.pop.opcode} and response opcode=${resp.bth.opcode} not match",
        severity = FAILURE
      )
    }
    psnRangeMatch
  })
  val hasPktToOutput = !txSelOH.orR
  when(psnOutRangeFifo.io.pop.valid && !hasPktToOutput) {
    // TODO: no output in OutPsnRange should be normal case
    report(
      message =
        L"${REPORT_TIME} time: no output packet in OutPsnRange: startPsn=${psnOutRangeFifo.io.pop.start}, endPsn=${psnOutRangeFifo.io.pop.end}, psnOutRangeFifo.io.pop.valid=${psnOutRangeFifo.io.pop.valid}",
      severity = FAILURE
    )
  }

  val txOutputSel = StreamOneHotMux(select = txSelOH.asBits(), inputs = txVec)
  psnOutRangeFifo.io.pop.ready := txOutputSel.bth.psn === psnOutRangeFifo.io.pop.end && txOutputSel.fire
  io.opsnInc.inc := txOutputSel.fire
  io.opsnInc.psnVal := txOutputSel.bth.psn

  val txDupRespVec = Vec(rxDupSendWriteResp, rxDupAtomicResp)
//    Vec(rxDupSendWriteResp, io.rxDupReadResp.pktFrag, rxDupAtomicResp)
  io.tx.pktFrag <-/< StreamArbiterFactory.roundRobin.fragmentLock
    .on(txDupRespVec :+ txOutputSel.continueWhen(hasPktToOutput))

  val isReadReq = OpCode.isReadReqPkt(psnOutRangeFifo.io.pop.opcode)
  val isAtomicReq = OpCode.isAtomicReqPkt(psnOutRangeFifo.io.pop.opcode)
  io.readAtomicRstCachePop.ready := txOutputSel.fire && txOutputSel.bth.psn === (io.readAtomicRstCachePop.psnStart + io.readAtomicRstCachePop.pktNum - 1)
  when(io.readAtomicRstCachePop.fire) {
    assert(
      assertion = isReadReq || isAtomicReq,
      message =
        L"${REPORT_TIME} time: the output should correspond to read/atomic resp, io.readAtomicRstCachePop.fire=${io.readAtomicRstCachePop.fire}, but psnOutRangeFifo.io.pop.opcode=${psnOutRangeFifo.io.pop.opcode}",
      severity = FAILURE
    )
    assert(
      assertion = psnOutRangeFifo.io.pop.fire,
      message =
        L"${REPORT_TIME} time: io.readAtomicRstCachePop.fire=${io.readAtomicRstCachePop.fire} and psnOutRangeFifo.io.pop.fire=${psnOutRangeFifo.io.pop.fire} should fire at the same time",
      severity = FAILURE
    )
  }
}