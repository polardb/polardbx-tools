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

public enum MaskType {
    HIDING,     // 掩码
    ENCRYPT,    // 加密
    HASH,       // 摘要
    FLOOR;      // 取整

    public static MaskType fromString(String type) {
        switch (type.toUpperCase()) {
        case "HIDING":
            return HIDING;
        case "ENCRYPT":
            return ENCRYPT;
        case "HASH":
            return HASH;
        case "FLOOR":
            return FLOOR;
        default:
            throw new UnsupportedOperationException("Unsupported mask type: " + type);
        }
    }
}
