package com.asiainfo.ocdp.flume.sink.redis;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by peng on 2016/11/14.
 */
public class RedisSinkUtils {

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
        return removeLastSeparator(new StringBuilder(value), separator);
    }

    public static String removeLastSeparator(StringBuilder value, String separator){
        int lastIndexSeparator = value.lastIndexOf(separator);
        if (lastIndexSeparator > 0){
            return value.deleteCharAt(lastIndexSeparator).toString();
        }

        return StringUtils.EMPTY;
    }
}
