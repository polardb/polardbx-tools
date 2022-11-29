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
 * 1. 对 PolarDB-X 的数据导出测试用例
 * 2. 需要手动验证导出文件的结果
 */
public class ShardingTableExportTest extends BaseJarTest {
    private static final Logger logger = LoggerFactory.getLogger(ShardingTableExportTest.class);

    private static String exportDirPath = null;

    private static final BaseDbConfig dbConfig = new BaseDbConfig(testProperties);

    private static final String SUB_DIR_NAME = "/data";
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
    public void exportTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s |", TABLE);
        runCommand(dbStr + opStr, exportDirPath + SUB_DIR_NAME);
        waitForExit();
    }

    @Test
    public void exportWithQuoteTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force", TABLE);
        runCommand(dbStr + opStr, exportDirPath + SUB_DIR_NAME);
        waitForExit();
    }

    @Test
    public void exportFixedFileNumTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o export -t %s -s , -quote force -F 3", TABLE);
        runCommand(dbStr + opStr, exportDirPath);
        waitForExit();
    }
}
