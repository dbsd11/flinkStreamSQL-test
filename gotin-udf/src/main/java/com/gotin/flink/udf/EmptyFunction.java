package com.gotin.flink.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author: bison
 * @Date: 20/9/18
 */
public class EmptyFunction extends ScalarFunction {

    public <T> T eval(T fromObj) {
        return fromObj;
    }
}
