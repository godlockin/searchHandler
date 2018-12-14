package com.tigerobo.searchhandler.common.component;


import com.tigerobo.searchhandler.common.LocalCache;
import com.tigerobo.searchhandler.common.constants.BusinessConstants;
import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.dao.BusinessDao;
import com.tigerobo.searchhandler.service.ESService;
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
    private ConcurrentHashMap<String, Map> taskCache = new ConcurrentHashMap<>();

    @Override
    public void run() {
        log.info("Start to monitor the data transport tasks");
        try {

            init();
            // start a monitor task to monitor this
            new Consumer().start();

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
                                String key = provinceCode + "_" + queryIndex;
                                Future<Long> taskFuture = executorService.submit(makeTask(config));
                                Map taskInfo = new HashMap() {{
                                    put("task", taskFuture);
                                    put("config", config);
                                }};
                                taskCache.put(key, new HashMap(taskInfo));
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

    private DataTransportCallable makeTask(Map config) {
        DataTransportCallable t = new DataTransportCallable();
        t.setBusinessDao(businessDao);
        t.setEsService(esService);

        t.setProvinceCode(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, ""));
        t.setProvinceName(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_NAME, String.class, ""));
        t.setPageSize(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PAGE_SIZE, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE));
        t.setEsIndex(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESINDEX, String.class, ""));
        t.setEsType(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESTYPE, String.class, ""));
        t.setIndex(DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_QUERY_INDEX, Long.class, 0L));
        return t;
    }

    class Consumer extends Thread {

        @Override
        public void run() {

            log.info("Start to consume the tasks");
            while (true) {
                taskCache.forEach((taskKey, taskConfig) -> {
                    Map config = (Map) taskConfig.get("config");
                    Future<Long> taskFuture = (Future) taskConfig.get("task");
                    log.info("Wait for result for config:[{}]", config);

                    try {
                        // get the query result
                        // remove the watching task
                        Long result = taskFuture.get();
                        taskCache.remove(taskKey);
                        if (0 >= result) {
                            log.error("Didn't get result for config:[{}]", config);
                        } else {
                            log.info("Handled {} data", result);
                        }

                        if (0 == waitCount.decrementAndGet()) {
                            log.info("Finished waiting for the tasks for now");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("Error happened when we call for the result for:[{}]", config);
                    }
                });
            }
        }
    }
}
