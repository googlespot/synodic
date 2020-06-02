package org.galaxy.synodic.affine;

public class ReqEvent {
    private volatile EventSession session;

    public EventSession getSession() {
        return session;
    }

    public void setSession(EventSession eventSession) {
        this.session = eventSession;
    }

}
