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

import common.BaseJarTest;
import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class NormalTableImportTest extends BaseJarTest {
    private static final Logger logger = LoggerFactory.getLogger(NormalTableImportTest.class);

    private static String importDirPath = null;

    private static final String TABLE = "customer";
    private static final String SUB_DIR_NAME = "data";
    /**
     * magic number from the csv text file
     */
    private static final int EXPECT_ROWS = 124;

    @BeforeClass
    public static void setUpNormalTableExportTest() {
        importDirPath = getProp("import_dir_path");
        if (StringUtils.isEmpty(importDirPath) || !new File(importDirPath).isDirectory()) {
            throw new RuntimeException("Please check import_dir_path config");
        }
    }

    @Before
    public void setUp() {
        try (Connection conn = getDbConn();
            Statement stmt = conn.createStatement()) {

            stmt.execute("TRUNCATE TABLE " + TABLE);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void before() {
        logger.info("start loading {}", TABLE);
    }

    @After
    public void after() {
        logger.info("loading {} done", TABLE);
        try (Connection conn = getDbConn();
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {

            Assert.assertTrue(rs.next());
            int rows = rs.getInt(1);
            Assert.assertEquals("Table row count does not match",
                EXPECT_ROWS, rows);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        logger.info("checking {} row count done", TABLE);
    }

    @Test
    public void importInDirTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s | -dir %s", TABLE, SUB_DIR_NAME);
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }

    /**
     * 原始文件的字段值不使用引号括起
     * 但导入时使用引号模式
     */
    @Test
    public void importInDirWithQuoteTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -dir %s -quote force", TABLE, SUB_DIR_NAME);
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }

    /**
     * 字段包含特殊字符（分隔符、换行符）的测试
     */
    @Test
    public void importInFileWithQuoteTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -f %s -quote force", TABLE,
            importDirPath + "/customer-quoted.data");
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }

    @Test
    public void importWithMediumBatchSizeTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -f %s -quote force -batchSize 100", TABLE,
            importDirPath + "/customer-quoted.data");
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }

    @Test
    public void importWithLargeBatchSizeTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -f %s -quote force -batchSize 300", TABLE,
            importDirPath + "/customer-quoted.data");
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }

    /**
     * 导入excel测试
     */
    @Test
    public void importXlsxTest() {
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -f %s -quote force -format XLSX", TABLE,
            importDirPath + "/customer_0.xlsx");
        runCommand(dbStr + opStr, importDirPath);
        waitForExit();
    }
}
