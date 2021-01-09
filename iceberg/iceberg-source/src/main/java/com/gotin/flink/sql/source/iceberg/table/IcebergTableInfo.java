package com.gotin.flink.sql.source.iceberg.table;

import com.dtstack.flink.sql.table.AbstractSourceTableInfo;
import com.google.common.base.Preconditions;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class IcebergTableInfo extends AbstractSourceTableInfo {

    private String warehouseLocation;

    private String icebergTableName;

    private String readFormat;

    public String getWarehouseLocation() {
        return warehouseLocation;
    }

    public void setWarehouseLocation(String warehouseLocation) {
        this.warehouseLocation = warehouseLocation;
    }

    public String getIcebergTableName() {
        return icebergTableName;
    }

    public void setIcebergTableName(String icebergTableName) {
        this.icebergTableName = icebergTableName;
    }

    public String getReadFormat() {
        return readFormat;
    }

    public void setReadFormat(String readFormat) {
        this.readFormat = readFormat;
    }

    @Override
    public boolean check() {
        Preconditions.checkNotNull(warehouseLocation, "Iceberg field of warehouseLocation is required");
        Preconditions.checkNotNull(icebergTableName, "Iceberg field of icebergTableName is required");
        return true;
    }

    @Override
    public String getType() {
        return "iceberg";
    }
}
