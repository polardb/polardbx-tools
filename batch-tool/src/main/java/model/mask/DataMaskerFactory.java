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

import com.alibaba.fastjson2.JSONObject;

public class DataMaskerFactory {

    private static final String MASK_TYPE_KEY = "type";

    /**
     * 掩码脱敏
     */
    private static final String HIDING_SHOW_END_KEY = "show_end";
    private static final String HIDING_SHOW_REGION_KEY = "show_region";


    /**
     * 哈希脱敏
     */
    private static final String HASH_SALT_KEY = "salt";

    public static AbstractDataMasker getDataMasker(JSONObject jsonConfig) {
        MaskType maskType = MaskType.fromString(jsonConfig.getString(MASK_TYPE_KEY));
        switch (maskType) {
        case HIDING:
            return buildHidingMasker(jsonConfig);
        case ENCRYPT:
            return buildEncryptMasker(jsonConfig);
        case HASH:
            return buildHashMasker(jsonConfig);
        case FLOOR:
            return buildFloorMasker(jsonConfig);
        default:
            throw new UnsupportedOperationException("Unsupported mask type: " + maskType);
        }
    }

    private static AbstractDataMasker buildHidingMasker(JSONObject jsonConfig) {
        HidingMasker hidingMasker = new HidingMasker();
        boolean hasShowOption = false;
        if (jsonConfig.containsKey(HIDING_SHOW_END_KEY)) {
            hidingMasker.setShowEndRegion(jsonConfig.getIntValue(HIDING_SHOW_END_KEY));
            hasShowOption = true;
        }
        if (jsonConfig.containsKey(HIDING_SHOW_REGION_KEY)) {
            hidingMasker.setShowRegions(jsonConfig.getString(HIDING_SHOW_REGION_KEY));
            hasShowOption = true;
        }
        if (!hasShowOption) {
            throw new IllegalArgumentException("Hiding masker requires at least one show region");
        }
        return hidingMasker;
    }

    private static AbstractDataMasker buildEncryptMasker(JSONObject jsonConfig) {
        throw new UnsupportedOperationException("Encrypt masker is not implemented yet");
    }

    private static AbstractDataMasker buildHashMasker(JSONObject jsonConfig) {
        HashMasker hashMasker = new HashMasker();
        if (jsonConfig.containsKey(HASH_SALT_KEY)) {
            hashMasker.setSalt(jsonConfig.getString(HASH_SALT_KEY));
        }

        return hashMasker;
    }

    private static AbstractDataMasker buildFloorMasker(JSONObject jsonConfig) {
        throw new UnsupportedOperationException("Floor masker is not implemented yet");
    }
}
