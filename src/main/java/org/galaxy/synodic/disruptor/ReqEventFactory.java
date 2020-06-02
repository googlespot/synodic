package org.galaxy.synodic.disruptor;

import com.lmax.disruptor.EventFactory;
import org.galaxy.synodic.affine.ReqEvent;

public class ReqEventFactory implements EventFactory<ReqEvent> {
    @Override
    public ReqEvent newInstance() {
        return new ReqEvent();
    }
}
