package model.encrypt;

import model.config.EncryptionConfig;
import org.bouncycastle.crypto.BufferedBlockCipher;
import org.bouncycastle.crypto.paddings.PaddedBufferedBlockCipher;
import org.bouncycastle.crypto.paddings.ZeroBytePadding;
import org.bouncycastle.crypto.params.KeyParameter;

public class AesCipher extends Cipher {
    private static final int KEY_LENGTH = 16;   // 128 bits

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
    public byte[] innerDecrypt(byte[] plainText) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void reset() {
        cipher.reset();
    }
}
