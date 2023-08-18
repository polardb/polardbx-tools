package com.aliyun.gts.sniffer.common.entity;

import com.alibaba.excel.annotation.ExcelProperty;
import com.alibaba.excel.annotation.format.NumberFormat;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode
public class ReportResult {
    @ExcelProperty("源库类型")
    private String sourceDbType;
    @ExcelProperty("目标库类型")
    private String dstDbType;
    @ExcelProperty("运行时长(秒)")
    private long execTime;
    @ExcelProperty("总请求数")
    private long reqCnt;
    @ExcelProperty("成功请求数")
    private long successReqCnt;
    @ExcelProperty("失败请求数")
    private long errReqCnt;
    @ExcelProperty("sql成功率")
    @NumberFormat("##.####%")
    private Double successReqRatio;
    @ExcelProperty("模板SQL数量")
    private long sqlTemplateCnt;
    @ExcelProperty("模板SQL成功数")
    private long sqlTemplateSuccessCnt;
    @ExcelProperty("模板SQL失败数")
    private long sqlTemplateErrCnt;
    @ExcelProperty("模板SQL成功率")
    @NumberFormat("##.####%")
    private Double sqlTemplateCompatibility;
    @ExcelProperty("平均执行时间(us)")
    private Double avgReqTime;

    @ExcelProperty("平均RT: >10s")
    private long avgReqTimeGT10S;

    @ExcelProperty("平均RT: 1s~10s")
    private long avgReqTimeGT1S;

    @ExcelProperty("平均RT: 100ms~1s")
    private long avgReqTimeGT100MS;

    @ExcelProperty("平均RT: 10ms~100ms")
    private long avgReqTimeGT10MS;

    @ExcelProperty("平均RT: 1ms~10ms")
    private long avgReqTimeGT1MS;

    @ExcelProperty("平均RT: <1ms")
    private long avgReqTimeLT1MS;
}


