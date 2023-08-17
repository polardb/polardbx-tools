package com.aliyun.gts.sniffer.common.entity;

public class MysqlBinaryValue {
    //字段类型
    private int type;
    //字段在preload的开始位置
    private int begin;
    //字段在preload的结束位置
    private int length;

    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    private byte[] data;

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

}

