package org.galaxy.synodic.disruptor;

import com.lmax.disruptor.RingBuffer;
import org.galaxy.synodic.affine.EventSession;
import org.galaxy.synodic.affine.ReqEvent;

import java.util.concurrent.ConcurrentHashMap;

public class ClientReqProcessor {
    private RingBuffer<ReqEvent>                  ringBuffer;
    private ConcurrentHashMap<Long, EventSession> sessionMap;

    public ClientReqProcessor(RingBuffer<ReqEvent> ringBuffer) {
        this.ringBuffer = ringBuffer;
    }

    public void handleReq(ClientRequest reqMsg) {
        EventSession session = sessionMap.get(reqMsg.getId());
        if (session == null) {
            int size = 4;
            if (reqMsg.getId() % 128 == 0) {
                size = 1024;
            }
            session = new EventSession(size);
            sessionMap.put(reqMsg.getId(), session);
        }
        long sequence = ringBuffer.next();
        try {
            ReqEvent event = ringBuffer.get(sequence);
            event.setSession(session);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
