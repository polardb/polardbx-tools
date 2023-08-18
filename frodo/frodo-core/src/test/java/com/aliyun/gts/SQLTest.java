package com.aliyun.gts;

import com.aliyun.gts.sniffer.common.utils.Util;
import org.testng.annotations.Test;

public class SQLTest {
    @Test
    public void test1(){
        String sql="/*+TDDL*\"++sdf234wefsfsdfsd\"/ select 1";
        String sql2= Util.trimHeaderHint(sql);
        System.out.println(sql2);
    }


}
