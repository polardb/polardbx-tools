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

/**
 * 可自定义且有默认参数的配置项
 */
public class BaseConfig {
    /**
     * 分隔符
     */
    protected String separator = ConfigConstant.DEFAULT_SEPARATOR;
    /**
     * 指定字符集
     */
    protected String charset = ConfigConstant.DEFAULT_CHARSET;

    /**
     * 第一行是否为字段名
     */
    protected boolean isWithHeader = ConfigConstant.DEFAULT_WITH_HEADER;

    protected boolean shardingEnabled;

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

    public String getCharset() {
        return charset;
    }

    public void setCharset(String charset) {
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

    @Override
    public String toString() {
        return "BaseConfig{" +
            "separator='" + separator + '\'' +
            ", charset='" + charset + '\'' +
            ", isWithHeader='" + isWithHeader + '\'' +
            '}';
    }
}
