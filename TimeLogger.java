package com.aol.identity.search;
/**
 * Created with IntelliJ IDEA.
 * User: aminerounak
 * Date: 10/22/13
 * Time: 1:32 PM
 * To change this template use File | Settings | File Templates.
 */

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimeLogger implements PostActivity {

    private final static Logger LOG = LoggerFactory.getLogger(TimeLogger.class);

    @Override
    public void postProcess(ESContext ctx) {
        ctx.getLogger().info("ES-{} took {} ms",
                ctx.getAction().toString().toUpperCase(),
                System.currentTimeMillis() - ctx.getStart());
    }
}
