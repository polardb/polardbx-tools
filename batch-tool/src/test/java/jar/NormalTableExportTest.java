/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package jar;

import common.BaseDbConfig;
import common.BaseJarTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * 1. 对单机MySQL 的数据导出测试用例
 * 2. 需要手动验证导出文件的结果
 */
public class NormalTableExportTest extends BaseJarTest {
    private static final Logger logger = LoggerFactory.getLogger(NormalTableExportTest.class);

    private static String exportDirPath = null;

    private static final BaseDbConfig dbConfig = new BaseDbConfig(testProperties);

    private static final String TABLE = "customer";

    @BeforeClass
    public static void setUpNormalTableExportTest() {
        exportDirPath = getProp("export_dir_path");
        if (StringUtils.isEmpty(exportDirPath) || !new File(exportDirPath).isDirectory()) {
            throw new RuntimeException("Please check export_dir_path config");
        }
    }

    @Before
    public void before() {
        logger.info("start exporting {}", TABLE);
    }

    @After
    public void after() {
        logger.info("exporting {} done", TABLE);
    }

    @Test
    public void exportWithQuoteTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }

    /**
     * 导出压缩文件
     */
    @Test
    public void exportGzipTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false -comp GZIP", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }

    /**
     * 导出Excel xlsx文件
     */
    @Test
    public void exportXlsxTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false -format XLSX", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }

    /**
     * 导出Excel xls文件
     */
    @Test
    public void exportXlsTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false -format XLS", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }

    /**
     * 掩码脱敏导出指定列
     */
    @Test
    public void exportHidingMaskTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false -mask {"
            + "\"c_phone\":{\"type\":\"hiding\",\"show_region\":\"0-2\",\"show_end\":4}"
            + "}", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }

    /**
     * 哈希+掩码脱敏导出指定列
     */
    @Test
    public void exportHashAndHidingMaskTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -sharding false -mask {"
            + "\"c_phone\":{\"type\":\"hiding\",\"show_region\":\"0-2\",\"show_end\":4},"
            + "\"c_name\":{\"type\":\"hash\",\"salt\":\"asdfgh\"}"
            + "}", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }
}
