package com.gotin.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * @Author: bison
 * @Date: 20/9/30
 */
public class RecognizeOriginFunction extends ScalarFunction {

    public String eval(String refererStr) {
        if (StringUtils.isNullOrWhitespaceOnly(refererStr)) {
            return null;
        }

        refererStr = refererStr.trim();
        refererStr = refererStr.startsWith("http://") ? refererStr.substring("http://".length()) : refererStr;
        refererStr = refererStr.startsWith("https://") ? refererStr.substring("https://".length()) : refererStr;
        refererStr = refererStr.contains("/") ? refererStr.substring(0, refererStr.indexOf("/")) : refererStr;
        return refererStr;
    }
}
