package com.tigerobo.searchhandler.service.impl;

import com.tigerobo.searchhandler.common.component.DataTransportThread;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
@Service
public class DataTransportServiceImpl implements DataTransportService {

    @Value("${paging.pageSize}")
    private Integer pageSize;
    private static int DEFAULT_PAGE_SIZE = 5000;

    @Value("${elasticsearch.index}")
    private String esIndex;
    @Value("${elasticsearch.type}")
    private String esType;

    @Autowired
    private ESService esService;
    @Autowired
    private BusinessDao businessDao;
    private ExecutorService executorService;

    @Override
    public Long provinceDump(String provinceCode) {
        Long count = 0L;
        if (StringUtils.isBlank(provinceCode) || BusinessConstants.PROVINCE_MAP.containsKey(provinceCode)) {
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
        param.put("provinceCode", provinceCode);
        param.put("provinceName", provinceName);
        if (pageSize < count) {
            long pageCount = count / pageSize;
            for (long index = 0; index < pageCount + 1; index++) {
                param.put("page", index * pageSize);
                param.put("size", pageSize);
                executorService.submit(buildDataTransportThread(param));
            }
        } else {
            executorService.submit(buildDataTransportThread(param));
        }
        log.info("[{}] data for [{}]-[{}] done", count, provinceCode, provinceName);
        return count;
    }

    @Override
    public Long fullDumpData() {
        log.info("Try to full dump data");

        long count = 0L;
        Iterator<Map.Entry<String, String>> it = BusinessConstants.PROVINCE_MAP.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String provinceCode = entry.getKey();
            count += provinceDump(provinceCode);
        }
        log.info("Handled {} data in total", count);
        return count;
    }

    private DataTransportThread buildDataTransportThread(Map<String, Object> param) {
        DataTransportThread t = new DataTransportThread();
        t.setBusinessDao(businessDao);
        t.setEsService(esService);
        t.setPageSize(pageSize);
        t.setEsIndex(esIndex);
        t.setEsType(esType);

        t.setIndex(DataUtils.getNotNullValue(param, "page", Long.class, 0L));
        t.setSize(DataUtils.getNotNullValue(param, "size", Integer.class, DEFAULT_PAGE_SIZE));
        t.setProvinceCode(DataUtils.getNotNullValue(param, "provinceCode", String.class, ""));
        t.setProvinceName(DataUtils.getNotNullValue(param, "provinceName", String.class, ""));
        return t;
    }

    @PostConstruct
    void init() {
        pageSize = DataUtils.handleNullValue(pageSize, Integer.class, DEFAULT_PAGE_SIZE);

        int poolSize = Runtime.getRuntime().availableProcessors() * 2;
        BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(1024);
        RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
        executorService = new ThreadPoolExecutor(poolSize, poolSize, 10, TimeUnit.SECONDS, queue, policy);
    }
}
