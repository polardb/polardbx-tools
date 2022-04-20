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
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.paddings.ZeroBytePadding;
import org.bouncycastle.crypto.params.KeyParameter;

public class AesCipher extends BaseCipher {

    private final BufferedBlockCipher cipher;

    public AesCipher(EncryptionConfig encryptionConfig, boolean encrypting) {
        super(encryptionConfig, encrypting);
        byte[] keyBytes = encryptionConfig.getKey().getBytes();
        byte[] key = new byte[KEY_LENGTH];
        System.arraycopy(keyBytes, 0, key, 0, Math.min(keyBytes.length, KEY_LENGTH));
        this.cipher = new PaddedBufferedBlockCipher(
            CipherUtil.getAesCipher(encryptionConfig.getEncryptionMode()), new ZeroBytePadding());
        cipher.init(encrypting, new KeyParameter(key));
    }

    @Override
    public byte[] innerEncrypt(byte[] plainText) throws Exception {
        byte[] crypto = new byte[cipher.getOutputSize(plainText.length)];
        int len = cipher.processBytes(plainText, 0, plainText.length, crypto, 0);
        cipher.doFinal(crypto, len);
        return crypto;
    }

    @Override
    protected byte[] innerDecrypt(byte[] crypto, int offset, int length) throws Exception {
        byte[] output = new byte[cipher.getOutputSize(crypto.length)];
        int len = cipher.processBytes(crypto, 0, crypto.length, output, 0);
        len += cipher.doFinal(output, len);
        byte[] result = new byte[len];
        System.arraycopy(output, 0, result, 0, result.length);
        return result;
    }

    @Override
    public void reset() {
        cipher.reset();
    }
}
