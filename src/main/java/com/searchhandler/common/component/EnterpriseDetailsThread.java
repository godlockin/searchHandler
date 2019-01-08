package com.searchhandler.common.component;

import com.searchhandler.common.constants.BusinessConstants.ESConfig;
import com.searchhandler.common.utils.DataUtils;
import com.searchhandler.service.ESService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;

@Data
@Slf4j
@Component
public class EnterpriseDetailsThread implements Callable<Integer> {

    private ConcurrentLinkedQueue<List> names = new ConcurrentLinkedQueue<>();
    private ESService esService;

    @Override
    public Integer call() {
        int count = 0;

        ExecutorService pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());

        try {
            List list = Optional.ofNullable(names.poll()).orElse(new ArrayList());
            while (!list.isEmpty()) {
                Map param = new HashMap();
                param.put(ESConfig.SIMPLE_QUERY_SIZE_KEY, list.size());
                Map terms = new HashMap();
                terms.put(ESConfig.SIMPLE_QUERY_TYPE_KEY, ESConfig.TERMS_KEY);
                terms.put(ESConfig.SIMPLE_QUERY_FIELD_KEY, "enterprise_name.keyword");
                terms.put(ESConfig.SIMPLE_QUERY_VALUE_KEY, list);
                List must = Collections.singletonList(terms);
                param.put(ESConfig.QUERY_KEY, new HashMap() {{
                    put(ESConfig.MUST_KEY, must);
                }});

                Map result = esService.complexSearch(param);
                List dataList = DataUtils.getNotNullValue(result, "data", List.class, new ArrayList<>());
                int size = dataList.size();
                log.info("Got {} data for {} base info", size, list.size());
                if (0 < size) {
                    List additional = pool.submit(new EnterpriseDetailsCacheThread(dataList)).get();
                    names.add(additional);
                    count += additional.size();
                }
                count += size;
                list = Optional.ofNullable(names.poll()).orElse(new ArrayList());
            }

            log.info("Handled {} companies' info", count);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we try to query enterprise info for:[{}]", names);
            count = 0;
        }
        return count;
    }

    public EnterpriseDetailsThread setNames(List names) {
        this.names.addAll(names);
        return this;
    }

    public EnterpriseDetailsThread setESService(ESService esService) {
        this.esService = esService;
        return this;
    }
}
