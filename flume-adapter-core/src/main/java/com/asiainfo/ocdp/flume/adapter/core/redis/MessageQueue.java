package com.asiainfo.ocdp.flume.adapter.core.redis;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by peng on 2016/11/10.
 */
public abstract class MessageQueue<T> {
    protected BlockingQueue<T> messages = new LinkedBlockingQueue();

    public abstract Map<String, String> getProperties();

    public abstract T takeMessage();

    public abstract boolean produceMessage();

}
