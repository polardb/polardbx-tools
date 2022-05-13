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

package worker.common;

import com.google.common.util.concurrent.RateLimiter;
import com.lmax.disruptor.WorkHandler;
import model.ConsumerExecutionContext;
import model.config.ConfigConstant;

/**
 * 限流代理类
 */
public abstract class BaseWorkHandler implements WorkHandler<BatchLineEvent> {

    protected ConsumerExecutionContext consumerContext;
    private RateLimiter rateLimiter = null;
    protected boolean hasEscapedQuote = false;
    protected String sep;
    /**
     * TODO tableName 从 map 取出的内容cache在独立context中
     */
    protected String tableName;

    protected void initLocalVars() {
        if (consumerContext.isUseMagicSeparator()) {
            this.sep = ConfigConstant.MAGIC_CSV_SEP;
            hasEscapedQuote = true;
        } else {
            this.sep = consumerContext.getSeparator();
            hasEscapedQuote = false;
        }
    }

    public void setConsumerContext(ConsumerExecutionContext consumerContext) {
        this.consumerContext = consumerContext;
    }

    public void createTpsLimiter(double tpsLimit) {
        if (tpsLimit > 0) {
            this.rateLimiter = RateLimiter.create(tpsLimit);
        }
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void onEvent(BatchLineEvent event) {
        // 保守起见，使用阻塞锁，不自旋
        if (rateLimiter != null) {
            rateLimiter.acquire(1);
        }
        onProxyEvent(event);
    }

    /**
     * 实际的事件处理函数
     */
    public abstract void onProxyEvent(BatchLineEvent event);
}
