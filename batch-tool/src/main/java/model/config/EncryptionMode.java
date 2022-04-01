package model.config;

public enum EncryptionMode {

    NONE,
    CAESAR,     // naive Caesar encryption
    AES_CBC;    // AES/CBC/PKCS5Padding

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
        default:
            throw new IllegalArgumentException("Unrecognized encryption mode: " + encryptionMode);
        }
    }
}
