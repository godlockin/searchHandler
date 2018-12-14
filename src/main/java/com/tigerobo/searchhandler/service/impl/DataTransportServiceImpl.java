package com.tigerobo.searchhandler.service.impl;

import com.tigerobo.searchhandler.common.LocalCache;
import com.tigerobo.searchhandler.common.component.DataTransportMonitorThread;
import com.tigerobo.searchhandler.common.constants.BusinessConstants;
import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.dao.BusinessDao;
import com.tigerobo.searchhandler.service.DataTransportService;
import com.tigerobo.searchhandler.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
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
    private DataTransportMonitorThread monitorThread;

    @Override
    public Long provinceDump(String provinceCode) {
        Long count = 0L;
        if (StringUtils.isBlank(provinceCode) || !BusinessConstants.PROVINCE_MAP.containsKey(provinceCode)) {
            log.error("No province info found");
            return count;
        }

        String provinceName = BusinessConstants.PROVINCE_MAP.get(provinceCode);
        count = businessDao.countForProvince(provinceCode);
        log.info("Got {} data for province:[{}]-[{}]", count, provinceCode, provinceName);
        if (0 == count) {
            log.error("No data found for province:[{}]-[{}]", provinceCode, provinceName);
            return count;
        }

        Map<String, Object> param = new HashMap<>();
        param.put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);
        param.put(BusinessConstants.QueryConfig.KEY_PROVINCE_NAME, provinceName);
        param.put(BusinessConstants.QueryConfig.KEY_PAGE_SIZE, pageSize);
        param.put(BusinessConstants.QueryConfig.KEY_ESINDEX, esIndex);
        param.put(BusinessConstants.QueryConfig.KEY_ESTYPE, esType);
        param.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, BusinessConstants.QueryConfig.DEFAULT_QUERY_INDEX);
        param.put(BusinessConstants.QueryConfig.KEY_QUERY_SIZE, pageSize);

        if (pageSize < count) {
            long pageCount = count / pageSize;
            for (long index = 0; index < pageCount + 1; index++) {
                param.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, index * pageSize);
                LocalCache.put(provinceCode, new HashMap<>(param));
            }
        } else {
            LocalCache.put(provinceCode, new HashMap<>(param));
        }

        log.info("[{}] data for [{}]-[{}] done", count, provinceCode, provinceName);
        return count;
    }

    @Override
    public Long fullDumpData() {
        log.info("Try to full dump data");

        long count = BusinessConstants.PROVINCE_MAP.keySet().stream()
                .collect(Collectors.summarizingLong(code -> provinceDump(code))).getSum();
        log.info("Handled {} data in total", count);
        return count;
    }

    @PostConstruct
    void init() {
        // init static variables
        pageSize = DataUtils.handleNullValue(pageSize, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        threadNum = DataUtils.handleNullValue(threadNum, Integer.class, BusinessConstants.QueryConfig.DEFAULT_THREAD_NUM);

        // init a monitor thread to provide & consume the query tasks
        monitorThread = new DataTransportMonitorThread();
        monitorThread.setBusinessDao(businessDao);
        monitorThread.setEsService(esService);
        monitorThread.setThreadNum(threadNum);
        monitorThread.start();
    }
}
