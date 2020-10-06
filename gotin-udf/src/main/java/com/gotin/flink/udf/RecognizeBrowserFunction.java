package com.gotin.flink.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * @Author: bison
 * @Date: 20/9/30
 */
public class RecognizeBrowserFunction extends ScalarFunction {

    public String eval(String fromStr) {
        if (StringUtils.isNullOrWhitespaceOnly(fromStr)) {
            return null;
        }

        UserAgent userAgent = UserAgent.parseUserAgentString(fromStr);
        return userAgent != null ? userAgent.toString() : null;
    }
}
