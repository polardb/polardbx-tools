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


public class FlagOption extends ConfigArgOption {

    public Boolean defaultValue = null;

    private FlagOption(String argShort, String argLong, String desc, String argName, Boolean defaultValue) {
        super(argShort, argLong, desc, argName);
        this.defaultValue = defaultValue;
    }

    /**
     * Boolean flag option
     */
    private static FlagOption of(String argShort, String argLong, String desc) {
        return of(argShort, argLong, desc, null);
    }

    private static FlagOption of(String argShort, String argLong, String desc, Boolean defaultValue) {
        return new FlagOption(argShort, argLong, desc, "true | false", defaultValue);
    }

    public static final FlagOption ARG_SHORT_LOAD_BALANCE =
        FlagOption.of("lb", "loadbalance",
            "Use jdbc load balance, filling the arg in $host like 'host1:port1,host2:port2' (default false).", false);
    public static final FlagOption ARG_SHORT_ENABLE_SHARDING =
        of("sharding", "sharding", "Whether enable sharding mode (default value depends on operation).");
    public static final FlagOption ARG_SHORT_WITH_HEADER =
        of("header", "header", "Whether the header line is column names (default false).", false);
    public static final FlagOption ARG_SHORT_IGNORE_AND_RESUME =
        of("i", "ignore", "Flag of insert ignore and resume breakpoint (default false).", false);
    public static final FlagOption ARG_SHORT_LOCAL_MERGE =
        of("local", "localMerge", "Use local merge sort (default false).", false);
    public static final FlagOption ARG_SHORT_SQL_FUNC =
        of("func", "sqlFunc", "Use sql function to update (default false).", false);
    public static final FlagOption ARG_SHORT_NO_ESCAPE =
        of("noEsc", "noEscape", "Do not escape value for sql (default false).", false);
    public static final FlagOption ARG_SHORT_READ_FILE_ONLY =
        of("rfonly", "readFileOnly", "Only read and process file, no sql execution (default false).", false);
    public static final FlagOption ARG_SHORT_USING_IN =
        of("in", "whereIn", "Using where cols `in [values]` (default true).", true);
    public static final FlagOption ARG_SHORT_WITH_LAST_SEP =
        of("lastSep", "withLastSep", "Whether line ends with separator (default false).", false);
    public static final FlagOption ARG_SHORT_PARALLEL_MERGE =
        of("para", "paraMerge", "Use parallel merge when doing order by export  (default false).", false);
    public static final FlagOption ARG_SHORT_PERF_MODE =
        of("perf", "perfMode", "Use performance mode at the sacrifice of compatibility (default false).", false);
    public static final FlagOption ARG_TRIM_RIGHT =
        of("trimRight", "trimRight", "Remove trailing whitespaces in a line for BlockReader (default false).", false);
    public static final FlagOption ARG_DROP_TABLE_IF_EXISTS =
        of("dropTableIfExists", "dropTableIfExists",
            "Add 'drop table if exists xxx' when exporting DDL (default false).",
            false);
    public static final FlagOption ARG_WITH_VIEW =
        of("withView", "withView", "Export views into files, or (default false).", false);
}
