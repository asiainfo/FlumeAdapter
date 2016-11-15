package com.asiainfo.ocdp.flume.sink.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.FlumeRedisUtils;
import org.apache.log4j.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Assembly {
    private Logger logger = Logger.getLogger(Assembly.class);

    protected String keyPrefix = null;
    protected String foreignKeys = null;
    private String hashFields = null;
    private String rowValue = null;
    private String rowSeparator = ",";
    private String rowSchema = null;
    private String rowSchemaSeparator = ",";

    protected String codisHashKey = null;
    private String[] rows = null;
    private List<String> rowHeaders = null;
    protected String[] hashFilesArray = null;
    protected String[] foreignKeysArray = null;


    protected Map<String, Map<String, String>> hmset = new HashMap();

    public void init(){
        this.rows = FlumeRedisUtils.stringToArrayBySeparator(rowValue, rowSeparator);
        this.rowHeaders = FlumeRedisUtils.stringToListBySeparator(rowSchema, rowSchemaSeparator);
        this.hashFilesArray = FlumeRedisUtils.stringToArrayBySeparator(hashFields, rowSchemaSeparator);
        this.foreignKeysArray = FlumeRedisUtils.stringToArrayBySeparator(foreignKeys, ",");
    }

    public Map<String, String> getMap() {
        HashMap<String, String> values = new HashMap();

        for (int j = 0; j < this.hashFilesArray.length; j++) {
            String hashValue = getColumnValueFromSourceTableRow(hashFilesArray[j].trim());
            if (hashValue != null) {
                values.put(hashFilesArray[j].trim(), hashValue);
            }
            else {
                logger.debug("Can not find " + hashFilesArray[j]);
            }
        }

        logger.debug("All fields are :" + values);

        return values;
    }

    public String getKey() {
        logger.debug("key:" + codisHashKey);
        return codisHashKey;
    }


    protected String getColumnValueFromSourceTableRow(String header) {
        int index = this.rowHeaders.indexOf(header);

        if (index < 0) {
            logger.trace("Can not find '" + header + "' from '" + this.rowHeaders + "'");
            return null;
        }
        return rows[index].trim();
    }


    public Map<String, Map<String, String>> getHmset() {
        hmset.put(getKey(), getMap());
        return hmset;
    }

    public abstract boolean execute();


    public Assembly setKeyPrefix(String keyPrefix) {
        this.keyPrefix = keyPrefix;
        return this;
    }

    public Assembly setForeignKeys(String foreignKeys) {
        this.foreignKeys = foreignKeys;
        return this;
    }

    public Assembly setHashFields(String hashFields) {
        this.hashFields = hashFields;
        return this;
    }

    public Assembly setRowValue(String rowValue) {
        this.rowValue = rowValue;
        return this;
    }

    public Assembly setRowSeparator(String rowSeparator) {
        this.rowSeparator = rowSeparator;
        return this;
    }

    public Assembly setRowSchema(String rowSchema) {
        this.rowSchema = rowSchema;
        return this;
    }

    @Override
    public String toString() {
        return "Assembly{" +
                "keyPrefix='" + keyPrefix + '\'' +
                ", foreignKeys='" + foreignKeys + '\'' +
                ", hashFields='" + hashFields + '\'' +
                ", rowValue='" + rowValue + '\'' +
                ", rowSeparator='" + rowSeparator + '\'' +
                ", rowSchema='" + rowSchema + '\'' +
                ", rowSchemaSeparator='" + rowSchemaSeparator + '\'' +
                '}';
    }
}
