package com.aliyun.gts.sniffer.common.entity;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class DefaultReport {
    @ExcelProperty("sqlId")
    private String sqlId;
    @ExcelProperty("Requests")
    private Long request;
    @ExcelProperty("Throughput (requests/second)")
    private Double reqPerSecond;
    @ExcelProperty("Request Errors")
    private Long errorReq;
    @ExcelProperty("request error/second")
    private Double errorReqPerSecond;
    @ExcelProperty("SrcDB Minimum RT(us)")
    private Double originMinRT;
    @ExcelProperty("Minimum RT(us)")
    private Double minRT;
    @ExcelProperty("SrcDB Average RT(us)")
    private Double originAvgRT;
    @ExcelProperty("Average RT(us)")
    private Double avgRT;
    @ExcelProperty("SrcDB Maximum RT(us)")
    private Double originMaxRT;
    @ExcelProperty("Maximum RT(us)")
    private Double maxRT;
    @ExcelProperty("schema")
    private String schema;
    @ExcelProperty("Sample Sql")
    private String sampleSql;
    @ExcelProperty("Error Msg")
    private String errorMsg;

//    @ExcelProperty("sql智能改写")
//    private String transferSql;
//    @ExcelProperty("sql改造项")
//    private String transferItem;
//    @ExcelProperty("sql不兼容项")
//    private String transferError;


}
