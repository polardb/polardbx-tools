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

import javax.annotation.concurrent.NotThreadSafe;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

@NotThreadSafe
public class HashMasker extends AbstractDataMasker {

    private static final int MAX_SALT_LENGTH = 16;
    private byte[] salt = null;

    private final MessageDigest MD5_DIGEST;
    private final Base64.Encoder BASE64_ENCODER = Base64.getEncoder();


    public HashMasker() {
        try {
            MD5_DIGEST = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MaskType getType() {
        return MaskType.HASH;
    }

    @Override
    public byte[] doMask(byte[] input) {
        if (salt == null) {
            return BASE64_ENCODER.encode(MD5_DIGEST.digest(input));
        }
        MD5_DIGEST.update(input);
        MD5_DIGEST.update(salt);
        return BASE64_ENCODER.encode(MD5_DIGEST.digest());
    }

    public void setSalt(byte[] salt) {
        if (this.salt != null) {
            throw new IllegalArgumentException("salt can only be initialized once");
        }
        Preconditions.checkNotNull(salt);
        Preconditions.checkArgument(salt.length <= MAX_SALT_LENGTH,
            "Hash salt max length is " + MAX_SALT_LENGTH);
        this.salt = salt;
    }

    public void setSalt(String salt) {
        setSalt(salt.getBytes(ConfigConstant.DEFAULT_CHARSET));
    }
}
