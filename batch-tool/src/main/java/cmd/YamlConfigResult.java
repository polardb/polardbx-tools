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
import org.yaml.snakeyaml.Yaml;
import util.FileUtil;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

public class YamlConfigResult implements ConfigResult {

    private final CommandLine commandLine;
    private final Map<String, Object> argMap;

    public YamlConfigResult(String yamlFilepath, CommandLine commandLine) {
        this.commandLine = commandLine;
        if (!FileUtil.canRead(yamlFilepath)) {
            throw new IllegalArgumentException("Cannot access yaml config file: " + yamlFilepath);
        }
        try {
            argMap = new Yaml().load(new FileInputStream(yamlFilepath));
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean hasOption(ConfigArgOption option) {
        return commandLine.hasOption(option.argShort) || hasYamlOption(option);
    }

    @Override
    public String getOptionValue(ConfigArgOption option) {
        // commandLine优先级高于yaml配置
        if (commandLine.hasOption(option.argShort)) {
            return commandLine.getOptionValue(option.argShort);
        }

        return getYamlOption(option);
    }

    private String getYamlOption(ConfigArgOption option) {
        String result = null;
        if (argMap.containsKey(option.argShort)) {
            result = String.valueOf(argMap.get(option.argShort));
        }
        if (argMap.containsKey(option.argLong)) {
            result = String.valueOf(argMap.get(option.argLong));
        }
        return result;
    }

    private boolean hasYamlOption(ConfigArgOption option) {
        return argMap.containsKey(option.argLong)
            || argMap.containsKey(option.argShort);
    }
}
