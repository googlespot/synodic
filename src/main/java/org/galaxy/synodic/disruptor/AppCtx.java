package org.galaxy.synodic.disruptor;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.galaxy.synodic.affine.ReqEvent;
import org.galaxy.synodic.affine.ReqEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AppCtx {
    private static final Logger              logger = LoggerFactory.getLogger(AppCtx.class);
    private              Disruptor<ReqEvent> disruptor;
    private              ClientReqProcessor  processor;

    public void init() {
        int ringBufferSize = 1024;
        disruptor = new Disruptor<>(
                new ReqEventFactory(),
                ringBufferSize,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new BlockingWaitStrategy());

        EventHandler<ReqEvent> eventHandler = (event, sequence, endOfBatch) ->
                new ReqEventHandler(e -> logger.info("evt={}", e)).onEvent(event);

        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        RingBuffer<ReqEvent> ringbuffer = disruptor.getRingBuffer();

        processor = new ClientReqProcessor(ringbuffer);
    }

    public void runTest() {
        ClientRequest req = new ClientRequest();
        processor.handleReq(req);
    }

    public void shutdown() {
        disruptor.shutdown();
    }

}
