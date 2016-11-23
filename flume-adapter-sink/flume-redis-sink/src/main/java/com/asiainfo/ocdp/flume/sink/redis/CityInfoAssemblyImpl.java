package com.asiainfo.ocdp.flume.sink.redis;

import org.apache.log4j.Logger;

import java.util.Map;

/**
 * Created by peng on 16/9/9.
 */
public class CityInfoAssemblyImpl extends SingleForeignKeysAssemblyImpl {
    private static Logger logger = Logger.getLogger(CityInfoAssemblyImpl.class);

    @Override
    public Map<String, Map<String, String>> getHmset() {
        String hmsetKey = getKey();
        Map<String, String> hmsetValue = getMap();

        if (hmsetKey.length() == 9){
            for (int i = 0; i <= 9 ; i++){
                hmset.put(hmsetKey + i, hmsetValue);
            }
        }
        else {
            hmset.put(hmsetKey, hmsetValue);
        }
        return hmset;
    }
}
