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

public abstract class BaseCipher {
    protected static final int KEY_LENGTH = 16;   // 128 bits

    protected final EncryptionConfig encryptionConfig;
    protected final boolean encrypting;

    BaseCipher(EncryptionConfig encryptionConfig, boolean encrypting) {
        this.encryptionConfig = encryptionConfig;
        this.encrypting = encrypting;
    }

    public final byte[] encrypt(byte[] plainText) throws Exception {
        if (!encrypting) {
            throw new IllegalStateException("Cannot encrypt in decryption mode");
        }
        return innerEncrypt(plainText);
    }

    protected abstract byte[] innerEncrypt(byte[] plainText) throws Exception;

    public final byte[] decrypt(byte[] crypto) throws Exception {
        if (encrypting) {
            throw new IllegalStateException("Cannot decrypt in encryption mode");
        }
        return innerDecrypt(crypto);
    }

    protected abstract byte[] innerDecrypt(byte[] crypto) throws Exception;

    public abstract void reset();

    public static BaseCipher getCipher(EncryptionConfig config, boolean encrypting) {
        switch (config.getEncryptionMode()) {
        case NONE:
            return null;
        case CAESAR:
            return new CaesarCipher(config, encrypting);
        case AES_CBC:
            return new AesCipher(config, encrypting);
        case SM4_ECB:
            return new Sm4Cipher(config, encrypting);
        default:
            throw new UnsupportedOperationException("Unsupported cipher: " + config.getEncryptionMode());
        }
    }

    /**
     * 支持定长块的加解密（加密前后字节数不变）
     */
    public boolean supportBlock() {
        return encryptionConfig.getEncryptionMode().isSupportStreamingBit();
    }
}
