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

package common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TestProperties {
    private static final String PROP_FILE_NAME = "config/conf.properties";
    private Properties properties = null;

    public TestProperties() {
        loadProps();
    }

    public String getProp(String key) {
        return properties.getProperty(key);
    }

    private void loadProps() {
        properties = new Properties();
        InputStream in = BaseJarTest.class.getClassLoader().getResourceAsStream(PROP_FILE_NAME);
        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Cannot load test config file");
        }
    }
}
