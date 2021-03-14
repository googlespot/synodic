package org.galaxy.synodic.disruptor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.galaxy.synodic.affine.EventSession;
import org.galaxy.synodic.affine.ReqEvent;
import org.galaxy.synodic.affine.ReqEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class AppCtx {
    private static final Logger                                   logger = LoggerFactory.getLogger(AppCtx.class);
    private              Disruptor<ReqEvent<?>>                   disruptor;
    private              ClientReqProcessor                       processor;
    private              ConcurrentHashMap<Long, EventSession<?>> sessionMap;

    public void init() {
        sessionMap = new ConcurrentHashMap<>();

        int ringBufferSize = 1024;
        disruptor = new Disruptor<>(new ReqEventFactory(), ringBufferSize, new ThreadFactoryBuilder().setNameFormat("worker-").build(), ProducerType.MULTI, new BlockingWaitStrategy());
        EventHandler<ReqEvent<?>> eventHandler = (event, sequence, endOfBatch) -> new ReqEventHandler(e -> logger.info("evt data={}", e.getEventData())).onEvent(event);

        disruptor.handleEventsWith(eventHandler);
        disruptor.start();

        RingBuffer<ReqEvent<?>> ringbuffer = disruptor.getRingBuffer();

        processor = new ClientReqProcessor(ringbuffer, sessionMap);
    }

    public void runTest(int id) {
        ClientRequest req = new ClientRequest();
        req.setId(id);
        req.setMsg("msg=" + id);
        processor.handleReq(req);
    }

    public void shutdown() {
        disruptor.shutdown();
    }

    public static void main(String[] args) {
        AppCtx appCtx = new AppCtx();
        appCtx.init();

        AtomicInteger idGen = new AtomicInteger(0);
        Executor executor = Executors.newFixedThreadPool(3);
        executor.execute(() -> {
            for (int i = 0; i < 10000; i++) appCtx.runTest(idGen.getAndIncrement());
        });

    }

}
