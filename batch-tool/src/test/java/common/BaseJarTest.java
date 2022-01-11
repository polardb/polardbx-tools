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

package common;

import org.apache.commons.lang3.StringUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

public class BaseJarTest {
    private static final Logger logger = LoggerFactory.getLogger(BaseJarTest.class);

    public static final String APP_NAME = "batch-tool.jar";
    protected static final TestProperties testProperties = new TestProperties();

    public static String CUR_WORK_DIR;
    public static String APP_PATH = null;
    protected Process process = null;
    protected BufferedReader outputReader = null;


    // TODO: timeout protection

    private static void loadAppPath() {
        if (StringUtils.isEmpty(APP_PATH)) {
            APP_PATH = getProp("app_path");
        }
        if (!StringUtils.isEmpty(APP_PATH)) {
            return;
        }
        String tmpPath = BaseJarTest.class.getResource("/").getPath();
        File parentDir = new File(tmpPath).getParentFile();
        if (!parentDir.exists()) {
            throw new RuntimeException("Please check batch-tool file path");
        }
        File jarFile = new File(parentDir.getAbsolutePath() + "/" + APP_NAME);
        if (!jarFile.exists() || !jarFile.isFile()) {
            throw new RuntimeException("Please check whether batch-tool.jar exists");
        }
        APP_PATH = jarFile.getAbsolutePath();
    }

    protected static String getProp(String key) {
        return testProperties.getProp(key);
    }

    @BeforeClass
    public static void preSetUp() {
        loadAppPath();
        logger.info("APP_PATH: {}", APP_PATH);
        File file = new File(APP_PATH);
        if (!file.exists() || !file.isFile()) {
            throw new RuntimeException("Please check batch-tool file path");
        }
        logger.info("check batch-tool jar path done...");
        CUR_WORK_DIR = System.getProperty("user.dir");
    }

    @Before
    public void setUp() {

    }

    @After
    public void cleanUp() {
        if (process != null && process.isAlive()) {
            waitForExit();
            Assert.assertFalse("Process is still alive!", process.isAlive());
        }
    }

    public void runCommand(String args) {
        runCommand(args, CUR_WORK_DIR);
    }

    public void runCommand(String args, String workingDirPath) {
        String cmd = String.format("java -jar %s %s",  APP_PATH, args);
        logger.info("Executing: {}", cmd);
        try {
            process = Runtime.getRuntime().exec(cmd, null, new File(workingDirPath));
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        outputReader = new BufferedReader(new InputStreamReader(process.getInputStream()));
    }

    protected String getNextOutputLine() {
        try {
          return outputReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail();
        }
        return "";
    }

    public void waitForExit() {
        int status = 0;
        try {
            status = process.waitFor();
            if (status != 0) {
                Assert.fail("Execute jar failed unexpectedly");
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
