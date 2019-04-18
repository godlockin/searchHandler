package searchhandler.common.component;

import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

@Data
@Slf4j
@Component
public class EnterpriseDetailsCallable extends FullCacheCallable {

    private ConcurrentLinkedQueue<List> names = new ConcurrentLinkedQueue<>();

    public EnterpriseDetailsCallable setNames(List names) {
        this.names.addAll(names);
        return this;
    }

    @Override
    public Long call() {
        long count = 0;

        pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());

        Map paramMap = new HashMap();
        Map result;
        List list = getNextList(new HashMap());
        try {
            while (!list.isEmpty()) {
                int size = list.size();
                log.debug("Got {} data", size);
                count += size;
                paramMap = buildParam(param, list);
                result = esService.complexSearch(paramMap);
                List dataList = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.DATA_KEY, List.class, new ArrayList<>());
                handleDetailResult(dataList);
                list = getNextList(new HashMap());
            }
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

    @Override
    protected int handleDetailResult(List list) {
        try {
            List additions = pool.submit(loopCacheHandler.apply(list)).get();
            if (names.add(additions)) {
                return list.size();
            }
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened");
        }

        return 0;
    }

    @Override
    protected Map buildParam(Map param, List lastResultList) {

        Map queryMap = new HashMap();
        queryMap.put(BusinessConstants.ESConfig.SIMPLE_QUERY_SIZE_KEY, lastResultList.size());
        Map terms = new HashMap();
        terms.put(BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, BusinessConstants.ESConfig.TERMS_KEY);
        terms.put(BusinessConstants.ESConfig.SIMPLE_QUERY_FIELD_KEY, "enterprise_name.keyword");
        terms.put(BusinessConstants.ESConfig.SIMPLE_QUERY_VALUE_KEY, lastResultList);
        List must = Collections.singletonList(terms);
        queryMap.put(BusinessConstants.ESConfig.QUERY_KEY, new HashMap() {{
            put(BusinessConstants.ESConfig.MUST_KEY, must);
        }});

        queryMap.put(BusinessConstants.ESConfig.SIMPLE_QUERY_INDEX_KEY, param.get(BusinessConstants.ESConfig.SIMPLE_QUERY_INDEX_KEY));
        queryMap.put(BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, param.get(BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY));

        return queryMap;
    }

    @Override
    protected Function<List, Callable<List>> getLoopCacheHandler() {
        return (list) -> new EnterpriseDetailsCacheCallable(list);
    }

    @Override
    protected List getNextList(Map result) {
        // get next trgt data by unique names instead of scroll
        return Optional.ofNullable(names.poll()).orElse(new ArrayList());
    }
}
