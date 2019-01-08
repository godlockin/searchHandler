package com.searchhandler.common.component;


import com.searchhandler.common.LocalCache;
import com.searchhandler.common.constants.BusinessConstants;
import com.searchhandler.dao.BusinessDao;
import com.searchhandler.service.ESService;
import com.searchhandler.common.utils.DataUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Slf4j
@Component
public class DataTransportMonitorThread extends Thread {

    private Integer threadNum;
    private ESService esService;
    private BusinessDao businessDao;
    private ExecutorService executorService;
    private AtomicInteger waitCount = new AtomicInteger(0);
    private ConcurrentLinkedQueue<Map> queue = new ConcurrentLinkedQueue<>();
    private Consumer consumer;

    @Override
    public void run() {
        log.info("Start to monitor the data transport tasks");
        try {

            init();

            while (true) {
                // loop the code and invoke the data transport tasks
                BusinessConstants.PROVINCE_MAP.keySet().forEach(code -> {
                    // keep up to 10 running task
                    if (threadNum > waitCount.get()) {
                        Map config = LocalCache.get(code);
                        if (!CollectionUtils.isEmpty(config)) {
                            // get the target task
                            Long queryIndex = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_QUERY_INDEX, Long.class, -1L);
                            String provinceCode = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, "");
                            if (StringUtils.isBlank(provinceCode) || 0L > queryIndex) {
                                log.error("Got config error:[{}]", config);
                            } else {
                                // count the task and start
                                waitCount.getAndIncrement();
                                queue.add(new HashMap() {{
                                    put("task", executorService.submit(new DataTransportCallable(config, esService, businessDao)));
                                    put("config", config);
                                }});

                                if (null == consumer) {
                                    consumer = new Consumer();
                                    consumer.start();
                                }
                            }
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we invoke the data transport tasks", e);
        }
    }

    private void init() {
        // init thread pool
        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1024);
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, queue, policy);
    }

    class Consumer extends Thread {

        @Override
        public void run() {

            log.info("Start to consume the tasks");
            int retry = 1000;
            while (true) {
                try {
                    Map taskConfig = Optional.ofNullable(queue.poll()).orElse(new HashMap());
                    if (taskConfig.isEmpty() && --retry > 0) {
                        Thread.sleep(1000);
                        continue;
                    } else if (0 == waitCount.decrementAndGet()) {
                        log.info("Finished waiting for the tasks for now");
                        break;
                    }

                    Map config = (Map) taskConfig.get("config");
                    Future<Long> taskFuture = (Future) taskConfig.get("task");
                    log.info("Wait for result for config:[{}]", config);

                    try {
                        // get the query result
                        // remove the watching task
                        Long result = taskFuture.get();
                        if (0 >= result) {
                            log.error("Didn't get result for config:[{}]", config);
                        } else {
                            log.info("Handled {} data", result);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Error happened when we call for the result for:[{}]", config);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened when we monitor the result");
                }
            }
        }
    }
}
