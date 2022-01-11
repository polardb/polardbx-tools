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

package preprocess;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import util.FileUtil;

public class SplitTest {

    private static final String SEP = ",";

    private static final String[]
        ORIGIN_VALS = {"301", "\"Wil\"\"lia\"\"m\"", "\"Gi\"\"\"\"e,tz\"", "\"WGIETZ\"", "\"515.\n123.8181\"",
        "1994-06-07 00:00:00", "AC_ACCOUNT", "8300.00", "\\N", "\"205\"", "110"};
    private static final String LINE = StringUtils.join(ORIGIN_VALS, SEP);

    private static final String[] EXPECTED_VALS = {"301", "Wil\"lia\"m", "Gi\"\"e,tz", "WGIETZ", "515.\n123.8181",
        "1994-06-07 00:00:00", "AC_ACCOUNT", "8300.00", "\\N", "205", "110"};


    @Test(expected = IllegalArgumentException.class)
    public void wrongWithoutLastSepTest1() {
        String sWithLastSep = LINE + SEP;

        FileUtil.split(sWithLastSep, SEP, false, ORIGIN_VALS.length, false);
    }

    @Test(expected = IllegalArgumentException.class)
    public void wrongWithLastSepTest1() {
        String sWrongCount = LINE;

        FileUtil.split(sWrongCount, SEP, true, ORIGIN_VALS.length + 1, false);
    }

    @Test()
    public void splitTest1() throws Exception {
        String s1 = LINE;
        String[] values1 = FileUtil.split(s1, SEP, false, ORIGIN_VALS.length, false);
        matchVals(values1);

        String s2 = s1 + SEP;
        String[] values2 = FileUtil.split(s2, SEP, true, ORIGIN_VALS.length, false);
        matchVals(values2);
    }



    private void matchVals(String[] vals) throws Exception {
        assert vals.length == EXPECTED_VALS.length;
        for (int i = 0; i < vals.length; i++) {
            if (!EXPECTED_VALS[i].equals(vals[i])) {
                throw new Exception(String.format("Expected: %s, got: %s",
                    EXPECTED_VALS[i], vals[i]));
            }
        }
    }
}
