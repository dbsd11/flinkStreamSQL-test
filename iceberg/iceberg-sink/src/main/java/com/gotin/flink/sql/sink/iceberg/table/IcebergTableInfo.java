package com.gotin.flink.sql.sink.iceberg.table;

import com.dtstack.flink.sql.table.AbstractTargetTableInfo;
import com.google.common.base.Preconditions;

/**
 * @Author: bison
 * @Date: 20/9/16
 */
public class IcebergTableInfo extends AbstractTargetTableInfo {

    private String warehouseLocation;

    private String icebergTableName;

    private Boolean isOverwrite;

    private Long writeDataFileBatch;

    private String writeFormat;

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

    public Boolean getOverwrite() {
        return isOverwrite;
    }

    public void setOverwrite(Boolean overwrite) {
        isOverwrite = overwrite;
    }

    public Boolean getIsOverwrite() {
        return isOverwrite;
    }

    public void setIsOverwrite(Boolean isOverwrite) {
        this.isOverwrite = isOverwrite;
    }

    public Long getWriteDataFileBatch() {
        return writeDataFileBatch;
    }

    public void setWriteDataFileBatch(Long writeDataFileBatch) {
        this.writeDataFileBatch = writeDataFileBatch;
    }

    public String getWriteFormat() {
        return writeFormat;
    }

    public void setWriteFormat(String writeFormat) {
        this.writeFormat = writeFormat;
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
