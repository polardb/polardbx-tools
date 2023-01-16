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

package datasource;

import util.DataSourceUtil;

public class DataSourceConfig {
    /**
     * 格式如
     * jdbc:mysql://%s:%s/%s
     */
    private String url;

    private String host;

    private String port;

    /**
     * 数据库名
     */
    private String dbName;

    private String username;

    private String password;

    private int maxConnectionNum;
    private int minConnectionNum;
    private int maxWait;
    private String connParam;
    private String initSqls;

    private boolean loadBalanceEnabled;

    public int getMaxConnectionNum() {
        return maxConnectionNum;
    }

    public void setMaxConnectionNum(int maxConnectionNum) {
        this.maxConnectionNum = maxConnectionNum;
    }

    public int getMinConnectionNum() {
        return minConnectionNum;
    }

    public void setMinConnectionNum(int minConnectionNum) {
        this.minConnectionNum = minConnectionNum;
    }

    public String getConnParam() {
        return connParam;
    }

    public void setConnParam(String connParam) {
        this.connParam = connParam;
    }

    public int getMaxWait() {
        return maxWait;
    }

    public static final class DataSourceConfigBuilder {
        private String host;
        private String port;
        private String dbName;
        private String username;
        private String password;
        private int minConnectionNum;
        private int maxConnectionNum;
        private int maxWait;
        private boolean loadBalanceEnabled;
        private String connParam;
        private String initSqls;

        public DataSourceConfigBuilder() {
        }

        public DataSourceConfigBuilder host(String host) {
            this.host = host;
            return this;
        }

        public DataSourceConfigBuilder port(String port) {
            this.port = port;
            return this;
        }

        public DataSourceConfigBuilder dbName(String dbName) {
            this.dbName = dbName;
            return this;
        }

        public DataSourceConfigBuilder username(String username) {
            this.username = username;
            return this;
        }

        public DataSourceConfigBuilder loadBalanceEnabled(boolean loadBalanceEnabled) {
            this.loadBalanceEnabled = loadBalanceEnabled;
            return this;
        }

        public DataSourceConfigBuilder password(String password) {
            this.password = password;
            return this;
        }

        public DataSourceConfigBuilder maxWait(int maxWait) {
            this.maxWait = maxWait;
            return this;
        }

        public DataSourceConfigBuilder maxConnNumber(int connNumber) {
            this.maxConnectionNum = connNumber;
            return this;
        }

        public DataSourceConfigBuilder minConnNumber(int connNumber) {
            this.minConnectionNum = connNumber;
            return this;
        }

        public DataSourceConfigBuilder connParam(String connParam) {
            this.connParam = connParam;
            return this;
        }

        public DataSourceConfigBuilder initSqls(String initSqls) {
            this.initSqls = initSqls;
            return this;
        }

        public DataSourceConfig build() {
            DataSourceConfig dataSourceConfig = new DataSourceConfig();
            dataSourceConfig.username = this.username;
            dataSourceConfig.host = this.host;
            dataSourceConfig.password = this.password;
            dataSourceConfig.port = this.port;
            dataSourceConfig.dbName = this.dbName;
            dataSourceConfig.maxConnectionNum = this.maxConnectionNum;
            dataSourceConfig.minConnectionNum = this.minConnectionNum;
            dataSourceConfig.maxWait = this.maxWait;
            dataSourceConfig.connParam = this.connParam;
            dataSourceConfig.initSqls = this.initSqls;
            dataSourceConfig.loadBalanceEnabled = this.loadBalanceEnabled;
            String jdbcUrl;
            if (loadBalanceEnabled) {
                jdbcUrl = String.format(DataSourceUtil.LOAD_BALANCE_URL_PATTERN,
                    host, dbName);
            } else {
                jdbcUrl = String.format(DataSourceUtil.URL_PATTERN,
                    host, port, dbName);
            }
            if (this.connParam != null) {
                jdbcUrl = jdbcUrl + "&" + connParam;
            }
            dataSourceConfig.url = jdbcUrl;
            return dataSourceConfig;
        }
    }

    public String getUrl() {
        return url;
    }

    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public String getDbName() {
        return dbName;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getInitSqls() {
        return initSqls;
    }

    @Override
    public String toString() {
        return "DataSourceConfig{" +
            "url='" + url + '\'' +
            ", host='" + host + '\'' +
            ", port='" + port + '\'' +
            ", dbName='" + dbName + '\'' +
            ", username='" + username + '\'' +
            ", password='" + password + '\'' +
            ", maxConnectionNum=" + maxConnectionNum +
            ", minConnectionNum=" + minConnectionNum +
            ", loadBalanceEnabled=" + loadBalanceEnabled +
            ", initSqls=" + initSqls +
            '}';
    }
}
