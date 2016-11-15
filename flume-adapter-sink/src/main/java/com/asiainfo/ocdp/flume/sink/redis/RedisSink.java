package com.asiainfo.ocdp.flume.sink.redis;

import io.codis.jodis.JedisResourcePool;
import io.codis.jodis.RoundRobinJedisPool;
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
    private String fileName = null;

    private String keySeparator = null;
    private String foreignKeysSeparator = null;


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
                }
                else {
                    String eventFileName = event.getHeaders().get(RedisSinkConstants.FILE_NAME);
                    if (StringUtils.equalsIgnoreCase(eventFileName, this.fileName)){
                        batchEvents.add(new String(event.getBody(), "UTF-8"));
                    }
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
        }

        return status;
    }

    @Override
    public void configure(Context context) {
        this.handlerClass = context.getString(RedisSinkConstants.HANDLER_CLASS);
        this.keyPrefix = context.getString(RedisSinkConstants.KEY_PREFIX);
        this.foreignKeys = context.getString(RedisSinkConstants.FOREIGN_KEYS);
        this.schema = context.getString(RedisSinkConstants.SCHEMA);
        this.hashFields = context.getString(RedisSinkConstants.HASH_FIELDS);

        this.zkAddress = context.getString(RedisSinkConstants.ZK_ADDRESS);
        this.zkSessionTimeout = context.getInteger(RedisSinkConstants.ZK_SESSION_TIMEOUT_MS);
        this.zkProxyDir = context.getString(RedisSinkConstants.ZK_PROXY_DIR);

        this.threadsPoolSize = context.getInteger(RedisSinkConstants.THREAD_POOL_SIZE);
        this.batchSize = context.getInteger(RedisSinkConstants.BATCH_SIZE, RedisSinkConstants.DEFAULT_BATCH_SIZE);

        this.fileName = context.getString(RedisSinkConstants.FILE_NAME);

        this.keySeparator = context.getString(RedisSinkConstants.KEY_SEPARATOR, RedisSinkConstants.DEFAULT_KEY_SEPARATOR);
        this.foreignKeysSeparator = context.getString(RedisSinkConstants.FOREIGNKEYS_SEPARATOR, RedisSinkConstants.DEFAULT_FOREIGNKEYS_SEPARATOR);
    }

    @Override
    public synchronized void start() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPool = RoundRobinJedisPool.create()
                .poolConfig(jedisPoolConfig)
                .curatorClient(zkAddress, zkSessionTimeout)
                .zkProxyDir(zkProxyDir).build();

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
                    .setRowSchema(this.schema)
                    .setKeySeparator(this.keySeparator)
                    .setForeignKeysSeparator(this.foreignKeysSeparator);
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
