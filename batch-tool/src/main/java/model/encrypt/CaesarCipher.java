package model.encrypt;

import model.config.EncryptionConfig;

public class CaesarCipher extends Cipher {

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
    protected byte[] innerDecrypt(byte[] crypto) throws Exception {
        byte[] plainText = new byte[crypto.length];
        for (int i = 0; i < plainText.length; i++) {
            plainText[i] = (byte) ((crypto[i] + mask) ^ mask);
        }
        return plainText;
    }

    @Override
    public void reset() {

    }
}