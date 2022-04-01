package model.encrypt;

import model.config.EncryptionConfig;

public abstract class Cipher {

    protected final EncryptionConfig encryptionConfig;
    protected final boolean encrypting;

    Cipher(EncryptionConfig encryptionConfig, boolean encrypting) {
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

    public static Cipher getCipher(EncryptionConfig config, boolean encrypting) {
        switch (config.getEncryptionMode()) {
        case NONE:
            return null;
        case CAESAR:
            return new CaesarCipher(config, encrypting);
        case AES_CBC:
            return new AesCipher(config, encrypting);
        default:
            throw new UnsupportedOperationException("Unsupported cipher: " + config.getEncryptionMode());
        }
    }
}
