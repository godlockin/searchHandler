package com.searchhandler.service.impl;

import com.alibaba.fastjson.JSON;
import com.searchhandler.common.LocalCache;
import com.searchhandler.dao.BusinessDao;
import com.searchhandler.service.DataTransportService;
import com.searchhandler.service.ESService;
import com.searchhandler.common.component.DataTransportMonitorThread;
import com.searchhandler.common.constants.BusinessConstants;
import com.searchhandler.common.constants.BusinessConstants.QueryConfig;
import com.searchhandler.common.utils.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataTransportServiceImpl implements DataTransportService {

    @Value("${paging.pageSize}")
    private Integer pageSize;
    @Value("${threadpool.keep-alive-num}")
    private Integer threadNum;

    @Value("${elasticsearch.index}")
    private String esIndex;
    @Value("${elasticsearch.type}")
    private String esType;

    @Autowired
    private ESService esService;
    @Autowired
    private BusinessDao businessDao;

    @Override
    public Long coreDump(Map<String, Object> config) {
        Long count = 0L;
        log.info("Try to do dump for:[{}]", JSON.toJSONString(config));
        String provinceCode = DataUtils.getNotNullValue(config, QueryConfig.KEY_PROVINCE_CODE, String.class, "");
        if (!(StringUtils.isNotBlank(provinceCode) && BusinessConstants.PROVINCE_MAP.containsKey(provinceCode))) {
            log.error("No province info found");
            return count;
        }

        String provinceName = BusinessConstants.PROVINCE_MAP.get(provinceCode);
        count = businessDao.countForProvince(config);
        log.info("Got {} data for province:[{}]-[{}]", count, provinceCode, provinceName);
        if (0 == count) {
            log.error("No data found for province:[{}]-[{}]", provinceCode, provinceName);
            return count;
        }

        Map<String, Object> param = new HashMap<>(config);
        param.put(QueryConfig.KEY_PAGE_SIZE, pageSize);
        log.info("Got query param:[{}]", param);

        // over size, we need to paging the queries
        if (pageSize < count) {
            long pageCount = count / pageSize;
            for (long index = 0; index < pageCount + 1; index++) {
                // set the query from index
                param.put(QueryConfig.KEY_QUERY_INDEX, index * pageSize);
                LocalCache.put(provinceCode, new HashMap<>(param));
            }
        }
        // otherwise, we ganna query once
        else {
            LocalCache.put(provinceCode, new HashMap<>(param));
        }

        log.info("[{}] data for [{}]-[{}] done", count, provinceCode, provinceName);
        return count;
    }

    @Override
    public Long dailyDump() {
        log.info("Try to full dump data");
        long count = doDumpData(doIncrementalDump());
        log.info("Handled {} data in total", count);
        return count;
    }

    @Override
    public Long fullDumpData() {
        log.info("Try to full dump data");
        long count = doDumpData(doFullDump());
        log.info("Handled {} data in total", count);
        return count;
    }

    private Long doDumpData(Function<String, Long> function) {
        log.info("Try to dump data");
        long count = BusinessConstants.PROVINCE_MAP.keySet().stream()
                .collect(Collectors.summarizingLong(function::apply)).getSum();
        log.info("Handled {} data in total", count);
        return count;
    }

    private Function<String, Long> doFullDump() {

        return (provinceCode) -> coreDump(new HashMap(){{put(QueryConfig.KEY_PROVINCE_CODE, provinceCode);}});
    }

    private Function<String, Long> doIncrementalDump() {

        return (provinceCode) -> {
            Map<String, Object> serialNoInfo = new HashMap() {{put(QueryConfig.KEY_PROVINCE_CODE, provinceCode);}};
            Map param = buildDailyAggParams(provinceCode);
            log.debug("Get agg query for:[{}]-[{}] as:[{}]", provinceCode,
                    BusinessConstants.PROVINCE_MAP.get(provinceCode), JSON.toJSONString(param));

            try {
                Map result = esService.complexSearch(param);
                Map agg = DataUtils.getNotNullValue(result, BusinessConstants.ESConfig.AGGREGATION_KEY, Map.class, new HashMap<>());
                serialNoInfo.put(BusinessConstants.DataDumpConfig.ESSENTIAL_INFORMATION_KEY, agg.get("essential_information.value"));
                serialNoInfo.put(BusinessConstants.DataDumpConfig.KEY_PERSONNEL_KEY, agg.get("key_personnel.kps_max.value"));
                serialNoInfo.put(BusinessConstants.DataDumpConfig.SHAREHOLDER_INFORMATION_KEY, agg.get("shareholder_information.sis_max.value"));

                serialNoInfo.put("essential_time", agg.get("essential_time.value"));
                serialNoInfo.put("key_personnel_time", agg.get("key_personnel_time.kpt_max.value"));
                serialNoInfo.put("shareholder_time", agg.get("shareholder_information_time.sit_max.value"));
            } catch (Exception e) {
                e.printStackTrace();
                String errMsg = "Error happened when we try to query ES as:" + e;
                log.error(errMsg);
            }

            log.info("Got the agg result for:[{}] as:", provinceCode, JSON.toJSONString(serialNoInfo));
            Long provinceCount = coreDump(serialNoInfo);
            log.info("Got {} incremental data for province:[{}]", provinceCount, provinceCode);
            return provinceCount;
        };
    }

    private Map buildDailyAggParams(String provinceCode) {
        // init param
        Map param = new HashMap();
        // no need data for dump
        param.put("size", 0);

        // build query
        // province_code must be ${provinceCode}
        Map query = new HashMap();
        List must = new ArrayList();
        Map queryItem = new HashMap();
        queryItem.put("type", "term");
        queryItem.put("field", "province_code");
        queryItem.put("value", provinceCode);
        must.add(queryItem);
        query.put("must", must);
        param.put("query", query);

        // build agg
        Map agg = new HashMap();
        // get max essential_information serialno
        Map ei = new HashMap();
        ei.put("type", "max");
        ei.put("name", "essential_information");
        ei.put("field", "serialno");
        agg.put("essential_information", ei);

        // get max essential_information create_time
        Map et = new HashMap();
        et.put("type", "max");
        et.put("name", "essential_time");
        et.put("field", "create_time");
        agg.put("essential_time", et);

        // get max key_personnel serialno
        // as the key_personnel is an nested field
        Map kp = new HashMap();
        kp.put("type", "nested");
        kp.put("name", "key_personnel");
        kp.put("path", "key_personnel");

        List kpSub = new ArrayList();
        Map kps = new HashMap();
        kps.put("name", "kps_max");
        kps.put("type", "max");
        kps.put("field", "key_personnel.serialno");
        kpSub.add(kps);
        kp.put("subAgg", kpSub);
        agg.put("key_personnel", kp);


        // get max key_personnel create_time
        // as the key_personnel is an nested field
        Map kpt = new HashMap();
        kpt.put("type", "nested");
        kpt.put("name", "key_personnel_time");
        kpt.put("path", "key_personnel");

        List kptSub = new ArrayList();
        Map kpts = new HashMap();
        kpts.put("name", "kpt_max");
        kpts.put("type", "max");
        kpts.put("field", "key_personnel.create_time");
        kptSub.add(kpts);
        kpt.put("subAgg", kptSub);
        agg.put("key_personnel_time", kpt);

        // get max shareholder_information serialno
        // as the shareholder_information is an nested field
        Map si = new HashMap();
        si.put("type", "nested");
        si.put("name", "shareholder_information");
        si.put("path", "shareholder_information");

        List siSub = new ArrayList();
        Map sis = new HashMap();
        sis.put("name", "sis_max");
        sis.put("type", "max");
        sis.put("field", "shareholder_information.serialno");
        siSub.add(sis);
        si.put("subAgg", siSub);
        agg.put("shareholder_information", si);


        // get max shareholder_information create_time
        // as the shareholder_information is an nested field
        Map sit = new HashMap();
        sit.put("type", "nested");
        sit.put("name", "shareholder_information_time");
        sit.put("path", "shareholder_information");

        List sitSub = new ArrayList();
        Map sits = new HashMap();
        sits.put("name", "sit_max");
        sits.put("type", "max");
        sits.put("field", "shareholder_information.create_time");
        sitSub.add(sits);
        sit.put("subAgg", sitSub);
        agg.put("shareholder_information_time", sit);

        param.put("aggregation", agg);

        return param;
    }

    @PostConstruct
    void init() {
        // init static variables
        pageSize = DataUtils.handleNullValue(pageSize, Integer.class, QueryConfig.DEFAULT_PAGE_SIZE);
        threadNum = DataUtils.handleNullValue(threadNum, Integer.class, QueryConfig.DEFAULT_THREAD_NUM);

        // init a monitor thread to provide & consume the query tasks
        DataTransportMonitorThread monitorThread = new DataTransportMonitorThread();
        monitorThread.setBusinessDao(businessDao);
        monitorThread.setEsService(esService);
        monitorThread.setThreadNum(threadNum);
        monitorThread.start();
    }
}
