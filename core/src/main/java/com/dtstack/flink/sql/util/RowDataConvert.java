package com.dtstack.flink.sql.util;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.Row;

import java.sql.Timestamp;


/**
 * Company: www.dtstack.com
 *
 * @author xuchao
 * @date 2020-05-20
 */
public class RowDataConvert {

    public static RowData convertToBaseRow(Row row) {
        int length = row.getArity();
        GenericRowData genericRow = new GenericRowData(length);
        for (int i = 0; i < length; i++) {
            if (row.getField(i) instanceof String) {
                genericRow.setField(i, StringData.fromString((String) row.getField(i)));
            } else if (row.getField(i) instanceof Timestamp) {
                TimestampData newTimestamp = TimestampData.fromTimestamp(((Timestamp) row.getField(i)));
                genericRow.setField(i, newTimestamp);
            } else {
                genericRow.setField(i, row.getField(i));
            }
        }

        return genericRow;
    }

    public static Row convertToRow(GenericRowData rowData) {
        int length = rowData.getArity();
        Row row = new Row(length);
        for (int i = 0; i < length; i++) {
            if (rowData.getField(i) instanceof StringData) {
                row.setField(i, rowData.getField(i).toString());
            } else if (rowData.getField(i) instanceof TimestampData) {
                row.setField(i, ((TimestampData) rowData.getField(i)).toTimestamp());
            } else {
                row.setField(i, rowData.getField(i));
            }
        }
        return row;
    }
}
