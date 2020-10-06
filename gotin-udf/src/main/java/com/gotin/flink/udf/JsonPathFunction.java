package com.gotin.flink.udf;

import com.jayway.jsonpath.JsonPath;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * @Author: bison
 * @Date: 20/9/30
 */
public class JsonPathFunction extends ScalarFunction {

    public String eval(String fromStr, String jsonPath) {
        if (StringUtils.isNullOrWhitespaceOnly(fromStr)) {
            return null;
        }

        Object value = JsonPath.read(fromStr, jsonPath);
        return value == null ? null : String.valueOf(value);
    }
}
