package com.asiainfo.ocdp.flume.sink.redis;

import org.apache.log4j.Logger;

public class SingleForeignKeysAssemblyImpl extends Assembly {
    private Logger logger = Logger.getLogger(SingleForeignKeysAssemblyImpl.class);

    @Override
    public boolean execute() {
        if (foreignKeysArray.length != 1) {
            logger.error("The number of foreign keys dose not equal 1.");
            return false;
        }

        String codisHashKeyPostfix = getColumnValueFromSourceTableRow(foreignKeysArray[0]);
        if (codisHashKeyPostfix != null){
            codisHashKey = keyPrefix + ":" + codisHashKeyPostfix;
        }else {
            logger.error("Can not find the column '" + codisHashKeyPostfix);
            return false;
        }

        return true;
    }

}
