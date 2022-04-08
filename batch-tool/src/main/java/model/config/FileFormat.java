package model.config;

public enum FileFormat {
    NONE(""),
    TXT(".txt"),
    CSV(".csv"),
    XLSX(".xlsx");

    private final String suffix;

    FileFormat(String suffix) {
        this.suffix = suffix;
    }

    public static FileFormat fromString(String compressMode) {
        // NONE / TXT / CSV / XLXS
        switch (compressMode.toUpperCase()) {
        case "NONE":
            return NONE;
        case "TXT":
            return TXT;
        case "CSV":
            return CSV;
        default:
            throw new IllegalArgumentException("Unrecognized file format: " + compressMode);
        }
    }

    public String getSuffix() {
        return suffix;
    }
}
