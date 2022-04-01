package model.config;

public enum DdlMode {
    /**
     * 导出数据与DDL建表语句
     */
    WITH_DDL,
    /**
     * 仅导出DDL建表语句
     */
    DDL_ONLY,
    /**
     * 默认 不导出DDL建表语句
     */
    NO_DDL;

    public static DdlMode fromString(String ddlMode) {
        ddlMode = ddlMode.toUpperCase();
        // NONE / ONLY / WITH
        switch (ddlMode) {
        case "NONE":
            return NO_DDL;
        case "ONLY":
            return DDL_ONLY;
        case "WITH":
            return WITH_DDL;
        default:
            throw new IllegalArgumentException("Illegal ddl mode: " + ddlMode);
        }
    }
}