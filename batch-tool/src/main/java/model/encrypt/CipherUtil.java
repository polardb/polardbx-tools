package model.encrypt;

import model.config.EncryptionMode;
import org.bouncycastle.crypto.BlockCipher;
import org.bouncycastle.crypto.engines.AESEngine;
import org.bouncycastle.crypto.modes.CBCBlockCipher;
import org.bouncycastle.crypto.paddings.ZeroBytePadding;

public class CipherUtil {


    public static BlockCipher getAesCipher(EncryptionMode encryptionMode) {
        switch (encryptionMode) {
        case AES_CBC:
            return new CBCBlockCipher(new AESEngine());
        default:
            throw new IllegalArgumentException("Unsupported encryption mode: " + encryptionMode);
        }
    }
}
