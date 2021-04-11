package org.galaxy.synodic.disruptor;

import org.galaxy.synodic.disruptor.EventSession;

public class ReqEvent<T> {
    private volatile EventSession session;
    private          T            eventData;

    public T getEventData() {
        return eventData;
    }

    public void setEventData(T eventData) {
        this.eventData = eventData;
    }

    public EventSession getSession() {
        return session;
    }

    public void setSession(EventSession eventSession) {
        this.session = eventSession;
    }

}
