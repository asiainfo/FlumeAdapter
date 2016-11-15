package com.asiainfo.ocdp.flume.adapter.core.redis;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by peng on 2016/11/14.
 */
public class FlumeRedisUtils {

    public static List<String> stringToListBySeparator(String value, String separator){
        if (StringUtils.isEmpty(value)){
            return new ArrayList<String>();
        }

        return Arrays.asList(stringToArrayBySeparator(value, separator));
    }

    public static String[] stringToArrayBySeparator(String value, String separator){
        if (StringUtils.isEmpty(value)){
            return new String[0];
        }

        return value.split(separator, -1);
    }

    public static String removeLastSeparator(String value, String separator){
        return StringUtils.removeEnd(value, separator);
    }

    public static String removeLastSeparator(StringBuilder value, String separator){
        return removeLastSeparator(value.toString(), separator);
    }
}
