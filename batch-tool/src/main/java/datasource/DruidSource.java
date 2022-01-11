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

import com.alibaba.druid.pool.DruidDataSource;
import exception.DataSourceException;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class DruidSource {

    private static volatile DruidDataSource instance;
    private static DataSourceConfig dataSourceConfig;

    public static void setDataSourceConfig(DataSourceConfig dataSourceConfig) {
        DruidSource.dataSourceConfig = dataSourceConfig;
    }

    public static DruidDataSource getInstance() throws DataSourceException {
        if (instance == null) {
            synchronized (DruidSource.class) {
                if (instance == null) {
                    if (dataSourceConfig == null) {
                        throw new DataSourceException("Config uninitialized");
                    }
                    instance = new DruidDataSource();
                    instance.setAsyncCloseConnectionEnable(true);
                    instance.setUrl(dataSourceConfig.getUrl());
                    instance.setUsername(dataSourceConfig.getUsername());
                    instance.setPassword(dataSourceConfig.getPassword());
                    instance.setInitialSize(dataSourceConfig.getMinConnectionNum());
                    instance.setMinIdle(dataSourceConfig.getMinConnectionNum());
                    instance.setMaxActive(dataSourceConfig.getMaxConnectionNum());
                    instance.setConnectionInitSqls(initSqlStrToList(dataSourceConfig.getInitSqls()));
                    instance.setMaxWait(dataSourceConfig.getMaxWait());
                }
            }
        }
        return instance;
    }

    private static List<String> initSqlStrToList(String initSqls) {
        List<String> initSqlList = new ArrayList<>();
        if (initSqls != null && !StringUtils.isEmpty(initSqls)) {

            // The ASCII of ' " ' is 34 : 
            int sc = initSqls.charAt(0);
            char ec = initSqls.charAt(initSqls.length() - 1);
            String newInitSqls = initSqls;
            if ((sc == 34 && ec == 34) || (sc == 39 && ec == 39) || (sc == 96 && ec == 96)) {
                // The ASCII of " is 34 ;
                // The ASCII of ' is 39 ;
                // The ASCII of ` is 39 ;
                newInitSqls = initSqls.substring(1, initSqls.length() - 1);
            }
            String[] initSqlListItem = newInitSqls.trim().split(";");
            for (int i = 0; i < initSqlListItem.length; i++) {
                if (StringUtils.isEmpty(initSqlListItem[i])) {
                    continue;
                }
                initSqlList.add(initSqlListItem[i]);
            }
        }
        return initSqlList;
    }
}
