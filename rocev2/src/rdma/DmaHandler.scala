package rdma

import spinal.core._
import spinal.lib._

import ConstantSettings._

import scala.language.postfixOps

class ExternalDmaHandler(busWidth: BusWidth.Value) extends Component {
  val io = new Bundle {
    val dma = slave(DmaBus(busWidth))
  }

  // TODO: check DMA length > 0

  // TODO: connect to DMA IP
  io.dma.rd.resp <-/< io.dma.rd.req.translateWith {
    val dmaReadData = Fragment(DmaReadResp(busWidth))
    dmaReadData.setDefaultVal()
    dmaReadData.last := False
    dmaReadData
  }

  io.dma.wr.resp <-/< io.dma.wr.req.translateWith {
    DmaWriteResp().setDefaultVal()
  }
}

class QpDmaHandler(busWidth: BusWidth.Value) extends Component {
  val io = new Bundle {
    val flush = in(Bool())
    val sqHasPendingDmaReadReq = out(Bool())
//    val retryFlush = in(Bool())
    val dma = master(DmaBus(busWidth))
    val rqDmaBus = slave(RqDmaBus(busWidth))
    val sqDmaBus = slave(SqDmaBus(busWidth))
  }

  val dmaRdReqVec = Vec(io.sqDmaBus.dmaRdReqVec ++ io.rqDmaBus.dmaRdReqVec)
  val dmaWrReqVec = Vec(io.rqDmaBus.dmaWrReqVec ++ io.sqDmaBus.dmaWrReqVec)
  io.dma.rd.arbitReq(dmaRdReqVec)
  io.dma.rd.deMuxRespByInitiator(
    rqRead = io.rqDmaBus.read.resp,
    rqAtomicRead = io.rqDmaBus.atomic.rd.resp,
    sqRead = io.sqDmaBus.reqOut.resp
  )
  io.dma.wr.arbitReq(dmaWrReqVec)
  io.dma.wr.deMuxRespByInitiator(
    rqWrite = io.rqDmaBus.sendWrite.resp,
    rqAtomicWr = io.rqDmaBus.atomic.wr.resp,
    sqWrite = io.sqDmaBus.respIn.resp
//    sqWrite = io.sqDmaBus.readResp.resp,
//    sqAtomicWr = io.sqDmaBus.atomic.resp
  )

  // CSR: count the number of DMA read requests from SQ
  val numSqDmaReadReqCnt =
    Reg(UInt(MAX_PENDING_WORK_REQ_NUM_WIDTH bits)) init (0)
  val sqReadRespDone =
    io.dma.rd.resp.lastFire && io.dma.rd.resp.initiator === DmaInitiator.SQ_RD
  when(io.flush) {
    numSqDmaReadReqCnt := 0
  } elsewhen (io.sqDmaBus.reqOut.req.fire && !sqReadRespDone) {
    numSqDmaReadReqCnt := numSqDmaReadReqCnt + 1
  } elsewhen (!io.sqDmaBus.reqOut.req.fire && sqReadRespDone) {
    numSqDmaReadReqCnt := numSqDmaReadReqCnt - 1
  }
  io.sqHasPendingDmaReadReq := numSqDmaReadReqCnt =/= 0
}

// TODO: how to find out the order between receive WR and send WR?
//
// pp. 292 spec 1.4
// Due to the ordering rule guarantees of requests and responses
// for reliable services, the requester is allowed to write CQ
// completion events upon response receipt.
//
// pp. 292 spec 1.4
// The completion at the receiver is in the order sent (applies only to
// SENDs and RDMA WRITE with Immediate) and does not imply previous
// RDMA READs are complete unless fenced by the requester.
class WorkCompOut extends Component {
  val io = new Bundle {
//    val dmaWrite = slave(DmaWriteRespBus())
    val rqSendWriteWorkComp = slave(Stream(WorkComp()))
    val sqWorkComp = slave(Stream(WorkComp()))
    val sqWorkCompErr = slave(Stream(WorkComp()))
    val workCompPush = master(Stream(WorkComp()))
  }

  // TODO: flush WorkReqCache when error
  //      status := WorkCompStatus.WR_FLUSH_ERR.id
  // TODO: output WC in PSN order
  io.workCompPush <-/< io.sqWorkComp
  StreamSink(io.rqSendWriteWorkComp.payloadType) << io.rqSendWriteWorkComp
  StreamSink(io.sqWorkCompErr.payloadType) << io.sqWorkCompErr
//  StreamSink(io.dmaWrite.resp.payloadType) << io.dmaWrite.resp
}
