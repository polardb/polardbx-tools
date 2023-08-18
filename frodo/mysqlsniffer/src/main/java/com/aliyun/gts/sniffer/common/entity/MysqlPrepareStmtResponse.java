package com.aliyun.gts.sniffer.common.entity;

public class MysqlPrepareStmtResponse {
    private Byte status=0x00;
    private int statementId=0;
    private int numColumns=0;
    private int numParams=0;
    private byte[] prepareData;

    public String getStmtSql(){
        return new String(prepareData,5,prepareData.length-5);
    }

    public byte[] getPrepareData() {
        return prepareData;
    }

    public void setPrepareData(byte[] prepareData) {
        this.prepareData = prepareData;
    }

    public Byte getStatus() {
        return status;
    }

    public void setStatus(Byte status) {
        this.status = status;
    }

    public int getStatementId() {
        return statementId;
    }

    public void setStatementId(int statementId) {
        this.statementId = statementId;
    }

    public int getNumColumns() {
        return numColumns;
    }

    public void setNumColumns(int numColumns) {
        this.numColumns = numColumns;
    }

    public int getNumParams() {
        return numParams;
    }

    public void setNumParams(int numParams) {
        this.numParams = numParams;
    }
}
