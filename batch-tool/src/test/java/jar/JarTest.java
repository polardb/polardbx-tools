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
import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class JarTest extends BaseJarTest {

    @Test
    public void versionTest() {
        runCommand("--version");
        waitForExit();
        String pattern = "^\\d\\.\\d\\.\\d$";
        String line = getNextOutputLine();
        Assert.assertTrue(StringUtils.isNotBlank(line));
        String[] strs = line.split(": ");
        Assert.assertEquals(2, strs.length);
        String version = strs[1];
        Assert.assertTrue(Pattern.matches(pattern, version));
    }

    @Test
    public void helpTest() {
        runCommand("--help");
        waitForExit();
        String line = getNextOutputLine();
        Assert.assertTrue(line != null && line.startsWith("usage: "));
    }
}