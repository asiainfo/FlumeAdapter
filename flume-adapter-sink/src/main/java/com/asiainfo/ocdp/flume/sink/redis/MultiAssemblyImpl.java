package com.asiainfo.ocdp.flume.sink.redis;

import com.asiainfo.ocdp.flume.adapter.core.redis.FlumeRedisUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class MultiAssemblyImpl extends Assembly {
    private Logger logger = Logger.getLogger(MultiAssemblyImpl.class);

    @Override
    public boolean execute() {

        if (foreignKeysArray.length < 2) {
            logger.error("The number of foreign keys are less than two.");
            return false;

        }

        String codisHashKeyPostfix = getCodisHashKeyPostfix(foreignKeysArray);
        if (codisHashKeyPostfix != null){
            codisHashKey = keyPrefix + keySeparator + codisHashKeyPostfix;
        }
        else {
            logger.error("Can not find the columns '" + foreignKeys);
            return false;
        }

        return true;
    }

    protected String getCodisHashKeyPostfix(String[] foreignKeysArr) {
        StringBuilder bs = new StringBuilder();
        for (String foreignKey : foreignKeysArr){
            String foreignKeyValue = getColumnValueFromSourceTableRow(foreignKey.trim());
            if (StringUtils.isEmpty(foreignKeyValue)){
                return null;
            }
            bs.append(foreignKeyValue).append(foreignKeysSeparator);
        }

        return FlumeRedisUtils.removeLastSeparator(bs, foreignKeysSeparator);
    }

}
