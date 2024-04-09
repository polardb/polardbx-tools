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

package util;

import javax.validation.constraints.NotNull;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DataSourceUtil {

    /**
     * 1. 手动拼接Batch,无需rewriteBatch
     */
    public static String URL_PATTERN = "jdbc:mysql://%s:%s/%s?allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=1000"
        + "&socketTimeout=600000&maintainTimeStats=false&zeroDateTimeBehavior=convertToNull"
        + "&useLocalSessionState=true&readOnlyPropagatesToServer=false";

    public static String LOAD_BALANCE_URL_PATTERN = "jdbc:mysql:loadbalance://%s/%s?"
        + "loadBalanceAutoCommitStatementThreshold=5&allowPublicKeyRetrieval=true&useSSL=false&connectTimeout=1000"
        + "&socketTimeout=600000&loadBalanceBlacklistTimeout=900000"
        + "&useLocalSessionState=true&readOnlyPropagatesToServer=false";

    public static Statement createStreamingStatement(@NotNull Connection conn) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.setFetchSize(Integer.MIN_VALUE);
        return stmt;
    }
}
