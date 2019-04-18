package searchhandler.common.component;

import com.alibaba.fastjson.JSON;
import searchhandler.common.LocalConfig;
import searchhandler.service.ESService;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import searchhandler.exception.SearchHandlerException;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Data
@Slf4j
public class DataFixMonitorThread extends Thread {

    private Map param;
    private ESService esService;
    private Integer threadNum;
    private ExecutorService pool;
    private DataConsumer dataConsumer;
    private Map<String, String> trgtInfo = new HashMap<>();
    private ConcurrentLinkedQueue<List> queue = new ConcurrentLinkedQueue<>();

    public DataFixMonitorThread(Map param, ESService esService) {
        this.param = param;
        this.esService = esService;
        threadNum = LocalConfig.get(BusinessConstants.SysConfig.THREAD_POOL_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());
    }

    @Override
    public void run() {
        log.info("Start to monitor details info");

        // from index/type scroll into trgtIndex/trgtType
        String trgtIndex = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_TRGT_INDEX, String.class, "");
        String trgtType = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_TRGT_TYPE, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE);
        if (StringUtils.isBlank(trgtIndex)) {
            log.error("Can't find trgt index");
            return;
        }
        trgtInfo.put(BusinessConstants.QueryConfig.KEY_TRGT_INDEX, trgtIndex);
        trgtInfo.put(BusinessConstants.QueryConfig.KEY_TRGT_TYPE, trgtType);


        try {
            Map result = getResult("");
            Long total = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.TOTAL_KEY, Long.class, 0L);
            String scrollId = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.SCROLL_ID_KEY, String.class, "");
            if (StringUtils.isBlank(scrollId)) {
                log.error("Error happened during our scroll process");
                return;
            }

            log.info("Try to handle {} data in total", total);
            List dataList = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.DATA_KEY, List.class, new ArrayList<>());
            long count = 0L;
            int group = 0;
            while (!dataList.isEmpty()) {
//                queue.add(dataList);
                pool.submit(new DataFixMonitorCallable(new HashMap(trgtInfo), dataList, esService)).get();

                count += dataList.size();
                result = getResult(scrollId);
                dataList = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.DATA_KEY, List.class, new ArrayList<>());
//                if (null == dataConsumer) {
//                    dataConsumer = new DataConsumer();
//                    dataConsumer.start();
//                }
//                group++;
            }
            log.info("Cached {} data for {} groups", count, group);

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we do reindex for param:[{}]", JSON.toJSONString(param));
        }

    }

    class DataConsumer extends Thread {
        private AtomicInteger count = new AtomicInteger(0);

        @Override
        public void run() {

            int fixed = 0;
            int retry = 10;
            while (--retry > 0) {
                List dataList = Optional.ofNullable(queue.poll()).orElse(new ArrayList());
                try {
                    if (!dataList.isEmpty() && count.incrementAndGet() < threadNum) {
                        log.info("Do handle {} data", dataList.size());
                        fixed += pool.submit(new DataFixMonitorCallable(trgtInfo, dataList, esService)).get();
                        count.decrementAndGet();
                        retry++;
                    } else {
                        log.info("No data found, wait and retry");
                        Thread.sleep(1000 * 60);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    log.error("Error happened when we try to fix data" + e);
                }
            }
            log.info("Fixed {} data", fixed);
        }
    }

    private Map getResult(String scrollId) throws SearchHandlerException {

        Map queryMap = new HashMap();
        if (StringUtils.isNotBlank(scrollId)) {
            queryMap.put(BusinessConstants.ResultConfig.SCROLL_ID_KEY, scrollId);
        } else {
            queryMap.putAll(DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY, Map.class, new HashMap<>()));
            queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_SIZE, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_SIZE, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE));
        }
        queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_INDEX, String.class, BusinessConstants.ESConfig.DEFAULT_ES_INDEX));
        queryMap.put(BusinessConstants.QueryConfig.KEY_QUERY_TYPE, DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_TYPE, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE));
        queryMap.put(BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY, DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_SCROLL_TIME_VALUE));

        log.info("Do query for param:[{}]", queryMap);
        Map result = esService.complexSearch(queryMap);
        log.debug("Got result as:[{}]", result);
        return result;
    }
}
