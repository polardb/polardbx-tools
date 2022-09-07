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

package model.mask;

import com.google.common.base.Preconditions;
import model.config.ConfigConstant;
import model.config.GlobalVar;
import org.apache.commons.lang3.StringUtils;

import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

@NotThreadSafe
public class HidingMasker extends AbstractDataMasker {

    private Charset charset = ConfigConstant.DEFAULT_CHARSET;
    private final static char HIDING_CHAR = '*';

    /**
     * 展示末尾几位
     */
    private int showEndRegion = 0;
    /**
     * 展示区间
     */
    private int[] showRegions = null;

    @Override
    public MaskType getType() {
        return MaskType.HIDING;
    }

    @Override
    public byte[] doMask(byte[] input) {
        if (GlobalVar.IN_PERF_MODE) {
            doSimpleHiding(input);
            return input;
        }

        return doStringHiding(input);
    }

    private byte[] doStringHiding(byte[] input) {
        String inputStr = new String(input, charset);
        char[] chars = inputStr.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            if (isHidingChar(i, chars.length)) {
                chars[i] = HIDING_CHAR;
            }
        }
        ByteBuffer byteBuffer = charset.encode(CharBuffer.wrap(chars));
        return Arrays.copyOfRange(byteBuffer.array(), byteBuffer.position(),
            byteBuffer.limit());
    }

    private void doSimpleHiding(byte[] input) {
        for (int i = 0; i < input.length; i++) {
            if (isHidingChar(i, input.length)) {
                input[i] = HIDING_CHAR;
            }
        }
    }

    private boolean isHidingChar(int index, int totalLength) {
        if (index >= totalLength - showEndRegion) {
            return false;
        }
        if (showRegions != null) {
            for (int j = 0; j < showRegions.length; j += 2) {
                if (index >= showRegions[j] && index <= showRegions[j + 1]) {
                    return false;
                }
            }
        }
        return true;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setShowEndRegion(int showEndRegion) {
        this.showEndRegion = showEndRegion;
    }

    /**
     * 区间用,分隔
     * @param showRegions 0-2,4-5
     */
    public void setShowRegions(String showRegions) {
        Preconditions.checkArgument(!StringUtils.isBlank(showRegions), "Empty show region");
        String[] regionStrs = StringUtils.split(showRegions, ",");
        int[] regions = new int[regionStrs.length * 2];
        for (int i = 0; i < regionStrs.length; i++) {
            String[] pair = StringUtils.split(regionStrs[i], "-");
            Preconditions.checkArgument(pair.length == 2,
                "Illegal region format: " + regionStrs[i]);
            regions[i * 2] = Integer.parseInt(pair[0]);
            regions[i * 2 + 1] = Integer.parseInt(pair[1]);
        }
        this.showRegions = regions;
    }
}
