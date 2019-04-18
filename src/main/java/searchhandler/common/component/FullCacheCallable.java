package searchhandler.common.component;

import searchhandler.common.LocalConfig;
import searchhandler.service.ESService;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

@Data
@Slf4j
@Component
public class FullCacheCallable implements Callable<Long> {

    protected Map param;
    protected ESService esService;

    protected Integer threadNum;
    protected ExecutorService pool;
    protected AtomicInteger threadCount = new AtomicInteger(0);
    protected ConcurrentLinkedQueue<Future<List>> queue = new ConcurrentLinkedQueue<>();
    protected Function<List, Callable<List>> loopCacheHandler = getLoopCacheHandler();

    protected DataConsumer dataConsumer;

    @Override
    public Long call() {
        long count = 0;
        int group = 0;

        threadNum = LocalConfig.get(BusinessConstants.SysConfig.THREAD_POOL_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());

        Map paramMap = buildParam(param, new ArrayList());
        try {
            Map result = esService.complexSearch(paramMap);
            paramMap.put(BusinessConstants.ResultConfig.SCROLL_ID_KEY, result.get(BusinessConstants.ResultConfig.SCROLL_ID_KEY));

            String index = DataUtils.getNotNullValue(param, BusinessConstants.BusinessDataConfig.EQUITY_INDEX_KEY, String.class, BusinessConstants.BusinessDataConfig.DEFAULT_EQUITY_INDEX);
            String type = DataUtils.getNotNullValue(param, BusinessConstants.BusinessDataConfig.EQUITY_TYPE_KEY, String.class, BusinessConstants.BusinessDataConfig.DEFAULT_EQUITY_TYPE);

            List list = getNextList(result);
            while (!list.isEmpty()) {
                group++;
                int size = list.size();
                log.debug("Got {} data", size);
                count += size;

                FullDetailsGenerateCallable callable = new FullDetailsGenerateCallable(esService, new ArrayList(list));
                callable.setIndex(index);
                callable.setType(type);
                queue.add(pool.submit(callable));
                        // handleDetailResult(list);

                threadCount.incrementAndGet();
//                log.debug("Got {} additional data", additional);

                while (threadCount.get() > threadNum) {
                    log.info("Wait for consume threads");
                    Thread.sleep(1000 * 10);
                }

                if (null == dataConsumer) {
                    dataConsumer = new DataConsumer();
                    dataConsumer.start();
                }

                list = getNextList(esService.complexSearch(paramMap));
            }

            log.info("Handled {} companies' info for {} groups", count, group);
            if (null != pool) {
                pool.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we try to query enterprise info for:[{}]" + e, paramMap);
            count = 0;
        }
        return count;
    }

    class DataConsumer extends Thread {

        @Override
        public void run() {

            log.info("Start a thread to consume queue");
            int retry = 60;
            Future<List> future = Optional.ofNullable(queue.poll()).orElse(null);
            while (--retry > 0) {
                try {
                    if (null != future) {
                        List dataList = future.get();
                        log.info("Do build for {} data", dataList.size());
                        threadCount.decrementAndGet();
                        retry = 60;
                    } else {
                        log.info("No data for now");
                        Thread.sleep(1000 * 10);
                    }

                    future = Optional.ofNullable(queue.poll()).orElse(null);
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened when we try to fix data" + e);
                }
            }
            log.info("Done");
        }
    }

    protected int handleDetailResult(List list) {
        queue.add(pool.submit(loopCacheHandler.apply(list)));
        return list.size();
    }

    protected Map buildParam(Map param, List lastResultList) {

        Map queryMap = new HashMap();
        String scrollId = DataUtils.getNotNullValue(param, BusinessConstants.ResultConfig.SCROLL_ID_KEY, String.class, "");
        if (StringUtils.isNotBlank(scrollId)) {
            queryMap.put(BusinessConstants.ResultConfig.SCROLL_ID_KEY, scrollId);
        } else {
            queryMap.putAll(DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY, Map.class, new HashMap<>()));
            queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_SIZE, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_SIZE, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE));
        }
        queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_INDEX, String.class, BusinessConstants.ESConfig.DEFAULT_ES_INDEX));
        queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_TYPE, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_TYPE, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE));
        queryMap.put(BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY, DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_SCROLL_TIME_VALUE));

        List includes = Arrays.asList("enterprise_name", "shareholder_information");
        Map fetchSource = new HashMap() {{ put(BusinessConstants.ESConfig.INCLUDE_KEY, includes); }};
        queryMap.put(BusinessConstants.ESConfig.FETCHSOURCE_KEY, fetchSource);

        return queryMap;
    }

    protected List getNextList(Map result) {
        return DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.DATA_KEY, List.class, new ArrayList<>());
    }

    protected Function<List, Callable<List>> getLoopCacheHandler() {
        return (list) -> new FullDetailsCacheCallable(list);
    }

    public FullCacheCallable setESService(ESService esService) {
        this.esService = esService;
        return this;
    }

    public FullCacheCallable setParam(Map param) {
        this.param = param;
        return this;
    }
}
