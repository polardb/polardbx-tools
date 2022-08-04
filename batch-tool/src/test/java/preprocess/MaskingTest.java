/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package preprocess;

import model.mask.AbstractDataMasker;
import model.mask.HashMasker;
import model.mask.HidingMasker;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import java.util.Arrays;

@RunWith(Enclosed.class)
public class MaskingTest {

    public static class HidingMaskTest {
        private HidingMasker masker;

        @Before
        public void before() {
            this.masker = new HidingMasker();
        }

        @Test
        public void testRegionMarking() {
            String[] input = {
                "",
                "zxcv",
                "zxcv1234",
                "中文zxc你好4",
            };
            String[] output = {
                "",
                "zx**",
                "zx******",
                "中文******",
            };
            masker.setShowRegions("0-1");
            checkMaskingResult(input, output, masker);
        }

        @Test
        public void testRegionMarkingWithEnd() {
            String[] input = {
                "",
                "zxcv",
                "zxcv1234",
                "中文zxc你好",
            };
            String[] output = {
                "",
                "*xcv",
                "*****234",
                "****c你好",
            };
            masker.setShowEndRegion(3);
            checkMaskingResult(input, output, masker);
        }

        @Test
        public void testMultiRegionMarking() {
            String[] input = {
                "",
                "zxcv",
                "zxcv1234",
                "中文zxc你好4123",
                "中文zxc你好4123中文",
            };
            String[] output = {
                "",
                "zxcv",
                "zxc*1234",
                "中文z*c你***23",
                "中文z*c你*****中文",
            };
            masker.setShowRegions("0-2,4-5");
            masker.setShowEndRegion(2);
            checkMaskingResult(input, output, masker);
        }
    }

    public static class HashMaskTest {
        private HashMasker masker;

        @Before
        public void before() {
            this.masker = new HashMasker();
        }

        @Test
        public void testHashWithSalt() {
            masker.setSalt("abc");
            byte[] input1 = "zxcvbn中文".getBytes();
            byte[] input2 = "zxcvb中文".getBytes();

            byte[] output1 = masker.doMask(input1);
            byte[] output2 = masker.doMask(input2);
            Assert.assertFalse(Arrays.equals(output1, output2));
            Assert.assertArrayEquals(masker.doMask(input1), output1);

            HashMasker masker2 = new HashMasker();
            masker2.setSalt("def");
            Assert.assertFalse(Arrays.equals(
                masker.doMask(input1), masker2.doMask(input1)));
        }

    }

    private static void checkMaskingResult(String[] input, String[] output, AbstractDataMasker masker) {
        Assert.assertEquals("output count does not match", input.length,
            output.length);
        for (int i = 0; i < input.length; i++) {
            byte[] inputBytes = input[i].getBytes();
            byte[] outPutBytes = masker.doMask(inputBytes);
            String outputStr = new String(outPutBytes);
            Assert.assertEquals(output[i], outputStr);
        }
    }
}
