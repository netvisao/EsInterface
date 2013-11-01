package com.a.identity.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 10/22/13
 * Time: 1:33 PM
 * To change this template use File | Settings | File Templates.
 */

import org.slf4j.Logger;



public class ESContext {

    public static class Builder {
        private ESContext ctx = new ESContext();

        Builder action(Action arg) {
            ctx.action = arg;
            return this;
        }

        Builder logger(Logger action) {
            ctx.logger = action;
            return this;
        }

        Builder start(long arg) {
            ctx.start = arg;
            return this;
        }

        ESContext build() {
            return ctx;
        }

    }

    private Logger logger;
    private long start;
    private Action action;

    public Logger getLogger() {
        return logger;
    }

    public void setLogger(Logger logger) {
        this.logger = logger;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }
}



