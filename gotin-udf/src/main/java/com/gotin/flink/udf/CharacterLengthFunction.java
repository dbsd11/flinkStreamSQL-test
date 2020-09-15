package com.gotin.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.StringUtils;

/**
 * @Author: bison
 * @Date: 20/9/15
 */
public class CharacterLengthFunction extends ScalarFunction {

    public String eval(String fromStr){
        if(StringUtils.isNullOrWhitespaceOnly(fromStr)){
            return fromStr;
        }

        return fromStr.substring(0, Math.min(fromStr.length(), 2));
    }
}
