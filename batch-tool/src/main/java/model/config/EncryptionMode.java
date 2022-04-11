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

package model.config;

public enum EncryptionMode {

    NONE(true),
    CAESAR(true),     // naive Caesar encryption
    AES_CBC(false),    // AES/CBC/PKCS5Padding
    SM4_ECB(false);    // SM4/EBC/PKCS5Padding

    private final boolean supportStreamingBit;

    EncryptionMode(boolean supportStreamingBit) {
        this.supportStreamingBit = supportStreamingBit;
    }

    static EncryptionMode fromString(String encryptionMode) {
        switch (encryptionMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "DEFAULT": // TODO fix default option
        case "CAESAR":
            return CAESAR;
        case "AES":
        case "AES-CBC":
            return AES_CBC;
        case "SM4":
        case "SM4-ECB":
            return SM4_ECB;
        default:
            throw new IllegalArgumentException("Unrecognized encryption mode: " + encryptionMode);
        }
    }

    public boolean isSupportStreamingBit() {
        return supportStreamingBit;
    }
}
