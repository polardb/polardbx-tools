package model.config;

import java.util.Arrays;
import java.util.Objects;

public class EncryptionConfig {

    public static final EncryptionConfig NONE = new EncryptionConfig(EncryptionMode.NONE, "");

    private final EncryptionMode encryptionMode;
    private final String key;
    private final byte[] initVector;    // for future use

    EncryptionConfig(EncryptionMode encryptionMode, String key) {
        this.encryptionMode = encryptionMode;
        this.key = key;
        this.initVector = null;
    }

    public static EncryptionConfig parse(String encryptionModeStr, String key) {
        EncryptionMode encryptionMode = EncryptionMode.fromString(encryptionModeStr);
        if (encryptionMode == EncryptionMode.NONE) {
            return NONE;
        }
        return new EncryptionConfig(encryptionMode, key);
    }

    public EncryptionMode getEncryptionMode() {
        return encryptionMode;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        if (this.encryptionMode == EncryptionMode.NONE) {
            return "Non-encrypted";
        }
        return "{" +
            "encryptionMode=" + encryptionMode +
            ", key='" + key + '\'' +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EncryptionConfig that = (EncryptionConfig) o;
        if (encryptionMode == EncryptionMode.NONE && encryptionMode == that.encryptionMode) {
            return true;
        }
        return encryptionMode == that.encryptionMode && key.equals(that.key) && Arrays.equals(initVector,
            that.initVector);
    }

    @Override
    public int hashCode() {
        if (this.encryptionMode == EncryptionMode.NONE) {
            return this.encryptionMode.hashCode();
        }
        int result = Objects.hash(encryptionMode, key);
        result = 31 * result + Arrays.hashCode(initVector);
        return result;
    }
}
