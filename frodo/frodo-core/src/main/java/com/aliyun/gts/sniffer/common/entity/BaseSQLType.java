package com.aliyun.gts.sniffer.common.entity;


public enum BaseSQLType{
    DDL("DDL"),
    DML("DML"),
    DQL("DQL"),
    OTHER("OTHER");
    private String val;
    private BaseSQLType(String str){
        this.val=str;
    }
    public String getVal(){
        return  this.val;
    }
}