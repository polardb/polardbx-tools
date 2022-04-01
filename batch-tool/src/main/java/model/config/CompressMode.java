package model.config;

public enum CompressMode {
    NONE,
    GZIP;

    public static CompressMode fromString(String compressMode) {
        // NONE / GZIP
        switch (compressMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "GZIP":
            return GZIP;
        default:
            throw new IllegalArgumentException("Unrecognized compression mode: " + compressMode);
        }
    }
}
