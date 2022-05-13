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

package model.encrypt;

import model.config.EncryptionConfig;

public class CaesarCipher extends BaseCipher {

    private byte mask;

    public CaesarCipher(EncryptionConfig encryptionConfig, boolean encrypting) {
        super(encryptionConfig, encrypting);
        this.mask = (byte) (encryptionConfig.getKey().hashCode() & 0xFF);
    }

    @Override
    protected byte[] innerEncrypt(byte[] plainText) throws Exception {
        byte[] crypto = new byte[plainText.length];
        for (int i = 0; i < plainText.length; i++) {
            crypto[i] = (byte) ((plainText[i] ^ mask) - mask);
        }
        return crypto;
    }

    @Override
    protected byte[] innerDecrypt(byte[] crypto, int offset, int length) throws Exception {
        byte[] plainText = new byte[length];
        for (int i = 0; i < plainText.length; i++) {
            plainText[i] = (byte) ((crypto[i] + mask) ^ mask);
        }
        return plainText;
    }

    @Override
    public void reset() {

    }
}