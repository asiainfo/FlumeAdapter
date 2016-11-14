package com.asiainfo.ocdp.flume.sink.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.JedisPoolFactory;
import io.codis.jodis.JedisResourcePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.log4j.Logger;
import redis.clients.jedis.JedisPoolConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by peng on 2016/11/14.
 */
public class RedisSink extends AbstractSink implements Configurable {
    private final static Logger logger = Logger.getLogger(RedisSink.class);

    private final JedisPoolFactory jedisPoolFactory;
    private JedisResourcePool jedisPool = null;
    private ExecutorService taskPool = null;
    private Assembly assembly = null;

    private String keyPrefix = null;
    private String foreignKeys = null;
    private String hashFields = null;
    private String schema = null;

    private String handlerClass = null;
    private String zkAddress = null;
    private Integer zkSessionTimeout = null;
    private String zkProxyDir = null;
    private Integer threadsPoolSize = null;
    private int batchSize;

    public RedisSink() {
        jedisPoolFactory = new JedisPoolFactory();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;

        if (jedisPool == null) {
            throw new EventDeliveryException("Redis connection not established. Please verify your configuration");
        }

        List<String> batchEvents = new ArrayList(batchSize);

        Channel channel = getChannel();
        Transaction txn = channel.getTransaction();

        try {
            txn.begin();

            for (int i = 0; i < batchSize && status != Status.BACKOFF; i++) {
                Event event = channel.take();
                if (event == null) {
                    status = Status.BACKOFF;
                } else {
                    batchEvents.add(new String(event.getBody(), "UTF-8"));
                }
            }

            /**
             * Only send events if we got any
             */
            if (batchEvents.size() > 0) {
                taskPool.execute(new RedisSinkTask(jedisPool, assembly, batchEvents));
            }

            txn.commit();
        } catch (Exception e) {
            txn.rollback();
            logger.error("Unexpected error", e);
        } finally {
            txn.close();
            try {
                jedisPool.close();
            } catch (IOException e) {
                logger.error("JedisResourcePool close failed", e);
            }
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        this.handlerClass = context.getString(RedisSinkConstants.HANDLER_CLASS);
        this.keyPrefix = context.getString(RedisSinkConstants.KEY_PREFIX);
        this.foreignKeys = context.getString(RedisSinkConstants.FOREIGN_KEYS);

        this.zkAddress = context.getString(RedisSinkConstants.ZK_ADDRESS);
        this.zkSessionTimeout = context.getInteger(RedisSinkConstants.ZK_SESSION_TIMEOUT_MS);
        this.zkProxyDir = context.getString(RedisSinkConstants.ZK_PROXY_DIR);

        this.threadsPoolSize = context.getInteger(RedisSinkConstants.THREAD_POOL_SIZE);
        this.batchSize = context.getInteger(RedisSinkConstants.BATCH_SIZE, RedisSinkConstants.DEFAULT_BATCH_SIZE);
    }

    @Override
    public synchronized void start() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPool = jedisPoolFactory.create(jedisPoolConfig, this.zkAddress, this.zkProxyDir, this.zkSessionTimeout);

        taskPool = Executors.newFixedThreadPool(this.threadsPoolSize);

        try {
            Class newoneClass = null;
            if (StringUtils.isNotEmpty(handlerClass)) {
                newoneClass = Class.forName(handlerClass);
            }
            else {
                //TODO
                if ("foreignKeys".length() == 1) {
                    newoneClass = Class.forName("");
                } else if ("foreignKeys".length() > 1) {
                    newoneClass = Class.forName("");
                } else {
                    logger.error("Can not determine handle class");
                }
            }

            assembly = (Assembly) newoneClass.newInstance();

            assembly.setKeyPrefix(this.keyPrefix)
                    .setForeignKeys(this.foreignKeys)
                    .setHashFields(this.hashFields)
                    .setRowSchema(this.schema);
        }catch (Exception e){
            logger.error(e);
        }


        super.start();
    }

    @Override
    public synchronized void stop() {
        if (jedisPool != null) {
            try {
                jedisPool.close();
            } catch (IOException e) {
                logger.error("JedisResourcePool close failed", e);
            }
        }

        if (taskPool != null){
            taskPool.shutdownNow();
        }
        super.stop();
    }
}
