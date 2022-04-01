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

package exec.export;

import cmd.BaseOperateCommand;
import cmd.ExportCommand;
import com.alibaba.druid.pool.DruidDataSource;
import datasource.DataSourceConfig;
import model.config.ExportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleThreadExportExecutor extends BaseExportExecutor {
    private static final Logger logger = LoggerFactory.getLogger(SingleThreadExportExecutor.class);

    private ExportCommand command;
    private ExportConfig config;

    public SingleThreadExportExecutor(DataSourceConfig dataSourceConfig,
                                      DruidDataSource druid,
                                      BaseOperateCommand baseCommand) {
        super(dataSourceConfig, druid, baseCommand);
    }

    @Override
    protected void setCommand(BaseOperateCommand baseCommand) {
        this.command = (ExportCommand) baseCommand;
        this.config = command.getExportConfig();
    }

    @Override
    void exportData() {
        doDefaultExport();
    }

    /**
     * 使用单条长连接导出数据
     */
    private void doDefaultExport() {
        throw new UnsupportedOperationException("no sharding export not supported yet");
    }
}
