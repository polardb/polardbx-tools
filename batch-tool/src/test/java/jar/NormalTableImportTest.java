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
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class NormalTableImportTest extends BaseJarTest {
    private static final Logger logger = LoggerFactory.getLogger(NormalTableImportTest.class);

    private static String importDirPath = null;

    private static final BaseDbConfig dbConfig = new BaseDbConfig(testProperties);
    private static final String TABLE = "customer";     // truncate target table first

    @BeforeClass
    public static void setUpNormalTableExportTest() {
        importDirPath = getProp("import_dir_path");
        if (StringUtils.isEmpty(importDirPath) || !new File(importDirPath).isDirectory()) {
            throw new RuntimeException("Please check import_dir_path config");
        }
    }

    @Test
    public void importInDirTest() {
        logger.info("start loading {}", TABLE);
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s | -dir %s", TABLE, importDirPath);
        runCommand(dbStr
            + opStr, importDirPath);
        waitForExit();

        logger.info("loading {} done", TABLE);
    }

    @Test
    public void importInDirWithQuoteTest() {
        logger.info("start loading {}", TABLE);
        String dbStr = String.format(" -h %s -P %s -u %s -p %s  -D %s", dbConfig.HOST, dbConfig.PORT,
            dbConfig.USER, dbConfig.PASSWORD, dbConfig.DB);
        String opStr = String.format(" -o import -t %s -s , -dir %s -quote force", TABLE, importDirPath);
        runCommand(dbStr
            + opStr, importDirPath);
        waitForExit();

        logger.info("loading {} done", TABLE);
    }
}
