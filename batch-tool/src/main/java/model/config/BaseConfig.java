/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package model.config;

import java.nio.charset.Charset;

/**
 * 可自定义且有默认参数的配置项
 * 导入导出通用配置
 */
public class BaseConfig {
    /**
     * 分隔符
     */
    protected String separator = ConfigConstant.DEFAULT_SEPARATOR;
    /**
     * 指定字符集
     */
    protected Charset charset = ConfigConstant.DEFAULT_CHARSET;

    /**
     * 第一行是否为字段名
     */
    protected boolean isWithHeader = ConfigConstant.DEFAULT_WITH_HEADER;

    protected boolean shardingEnabled;

    protected DdlMode ddlMode = DdlMode.NO_DDL;

    protected CompressMode compressMode = CompressMode.NONE;

    protected EncryptionConfig encryptionConfig = EncryptionConfig.NONE;

    protected FileFormat fileFormat = FileFormat.NONE;

    /**
     * 引号模式
     */
    protected QuoteEncloseMode quoteEncloseMode;

    public BaseConfig(boolean shardingEnabled) {
        this.shardingEnabled = shardingEnabled;
    }

    public String getSeparator() {
        return separator;
    }

    public void setSeparator(String separator) {
        // 分隔符不能包含特殊字符
        for (String illegalStr : ConfigConstant.ILLEGAL_SEPARATORS) {
            if (separator.contains(illegalStr)) {
                throw new IllegalArgumentException("Illegal separator: " + separator);
            }
        }
        this.separator = separator;
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public boolean isWithHeader() {
        return isWithHeader;
    }

    public void setWithHeader(boolean withHeader) {
        isWithHeader = withHeader;
    }

    public boolean isShardingEnabled() {
        return shardingEnabled;
    }

    public DdlMode getDdlMode() {
        return ddlMode;
    }

    public void setDdlMode(DdlMode ddlMode) {
        this.ddlMode = ddlMode;
    }

    public CompressMode getCompressMode() {
        return compressMode;
    }

    public void setCompressMode(CompressMode compressMode) {
        if (this.encryptionConfig.getEncryptionMode() != EncryptionMode.NONE && compressMode != CompressMode.NONE) {
            throw new UnsupportedOperationException("Do not support compression with encryption");
        }
        this.compressMode = compressMode;
    }

    public EncryptionConfig getEncryptionConfig() {
        return encryptionConfig;
    }

    public void setEncryptionConfig(EncryptionConfig encryptionConfig) {
        if (this.compressMode != CompressMode.NONE && encryptionConfig.getEncryptionMode() != EncryptionMode.NONE) {
            throw new UnsupportedOperationException("Do not support encryption with compression");
        }
        this.encryptionConfig = encryptionConfig;
    }

    public QuoteEncloseMode getQuoteEncloseMode() {
        return quoteEncloseMode;
    }

    public void setQuoteEncloseMode(QuoteEncloseMode quoteEncloseMode) {
        this.quoteEncloseMode = quoteEncloseMode;
    }

    public void setQuoteEncloseMode(String Mode) {
        this.quoteEncloseMode = QuoteEncloseMode.parseMode(Mode);
    }

    public FileFormat getFileFormat() {
        return fileFormat;
    }

    public void setFileFormat(FileFormat fileFormat) {
        if (this.compressMode != CompressMode.NONE && fileFormat != FileFormat.NONE) {
            throw new IllegalArgumentException("Do not support file format in compression mode");
        }
        this.fileFormat = fileFormat;
    }

    @Override
    public String toString() {
        return "BaseConfig{" +
            "separator='" + separator + '\'' +
            ", charset='" + charset + '\'' +
            ", isWithHeader='" + isWithHeader + '\'' +
            ", compressMode='" + compressMode + '\'' +
            ", encryptionConfig='" + encryptionConfig + '\'' +
            '}';
    }
}
