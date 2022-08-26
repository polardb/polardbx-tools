/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cmd;

import org.apache.commons.cli.CommandLine;

public class YamlConfigResult implements ConfigResult {

    private final CommandLine commandLine;

    public YamlConfigResult(String yamlFilepath, CommandLine commandLine) {
        this.commandLine = commandLine;
    }

    @Override
    public boolean hasOption(ConfigArgOption option) {
        return commandLine.hasOption(option.argShort);
    }

    @Override
    public String getOptionValue(ConfigArgOption option) {
        return commandLine.getOptionValue(option.argShort);
    }
}
