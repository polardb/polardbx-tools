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
import org.bouncycastle.jce.provider.BouncyCastleProvider;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;

/**
 * 国密sm4算法
 *
 */
public class Sm4Cipher extends BaseCipher {

    static {
        Security.addProvider(new BouncyCastleProvider());
    }
    public static final String DEFAULT_SM4_ALGORITHM = "SM4/ECB/PKCS5Padding";
    public static final String ALGORITHM_NAME = "SM4";

    private final Cipher cipher;

    public Sm4Cipher(EncryptionConfig encryptionConfig, boolean encrypting) {
        super(encryptionConfig, encrypting);
        try {
            cipher = Cipher.getInstance(Sm4Cipher.DEFAULT_SM4_ALGORITHM, BouncyCastleProvider.PROVIDER_NAME);
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException e) {
            throw new RuntimeException(e);
        }
        byte[] key = new byte[KEY_LENGTH];
        byte[] inputKey = encryptionConfig.getKey().getBytes();
        System.arraycopy(inputKey, 0, key, 0, Math.min(inputKey.length, key.length));
        Key sm4Key = new SecretKeySpec(key, ALGORITHM_NAME);
        try {
            cipher.init(encrypting ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, sm4Key);
        } catch (InvalidKeyException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected byte[] innerEncrypt(byte[] plainText) throws Exception {
        return cipher.doFinal(plainText);
    }

    @Override
    protected byte[] innerDecrypt(byte[] crypto, int offset, int length) throws Exception {
        return cipher.doFinal(crypto, offset, length);
    }

    @Override
    public void reset() {

    }
}
