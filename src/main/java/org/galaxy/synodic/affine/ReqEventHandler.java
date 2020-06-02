package org.galaxy.synodic.affine;

import com.google.common.util.concurrent.RateLimiter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;

public class ReqEventHandler {
    private static final Logger logger = LoggerFactory.getLogger(ReqEventHandler.class);

    private final RateLimiter limiter = RateLimiter.create(1000);

    private final Consumer<ReqEvent> eventHandler;

    //todo exception handler

    public ReqEventHandler(Consumer<ReqEvent> eventHandler) {
        this.eventHandler = eventHandler;
    }

    public void onEvent(ReqEvent event) {
        EventSession session = event.getSession();

        for (; ; ) {
            if (session.assign(this)) {
                try {
                    if (event != null) {
                        eventHandler.accept(event);
                        event = null;
                    }
                    for (ReqEvent evt = session.pollEvent(); evt != null; evt = session.pollEvent()) {
                        eventHandler.accept(evt);
                    }
                    long counter = session.getTryInCounter();
                    session.retrieve(this);
                    if (counter == session.getTryInCounter()) {
                        break;
                    }
                } catch (RuntimeException re) {
                    session.retrieve(this);
                    throw re;
                }
            } else if (event != null) {
                ReqEventHandler handler = session.getHandler();
                session.tryIn();
                if (handler != null && handler == session.getHandler()) {
                    if (session.addEvent(event)) {
                        break;
                    } else {
                        logger.warn("queue is full ,session={}", session);
                        Thread.yield();
                        limiter.acquire();
                    }
                }
            }
        }
    }


}
