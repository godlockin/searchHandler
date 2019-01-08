package com.searchhandler.service.impl;

import com.searchhandler.common.EnterpriseCache;
import com.searchhandler.common.EnterpriseQueue;
import com.searchhandler.common.LocalConfig;
import com.searchhandler.common.component.EnterpriseDetailsThread;
import com.searchhandler.common.constants.BusinessConstants;
import com.searchhandler.common.constants.ResultEnum;
import com.searchhandler.common.utils.DataUtils;
import com.searchhandler.exception.SearchHandlerException;
import com.searchhandler.service.ESService;
import com.searchhandler.service.EnterpriseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class EnterpriseServiceImpl implements EnterpriseService {

    private Integer pageSize;
    private ExecutorService pool;
    @Autowired
    private ESService esService;

    @Override
    public String buildData(String baseUrl, String trgtUrl) throws SearchHandlerException {
        trgtUrl = (StringUtils.isBlank(trgtUrl)) ? baseUrl + "_output.csv" : trgtUrl;
        log.info("Try to build info from:[{}] and write into:[{}]", baseUrl, trgtUrl);

        // prepare base companies
        String errMsg;
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(baseUrl)))){
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                EnterpriseQueue.recordBaseNames(tmp.replace(" ", "").trim());
            }
        } catch (Exception e) {
            e.printStackTrace();
            errMsg = "Error happened when we load base companies" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.FAILURE, errMsg);
        }

        // base company
        Set<String> baseNames = EnterpriseQueue.loadBaseNames();
        if (baseNames.isEmpty()) {
            return trgtUrl;
        }
        int size = baseNames.size();
        log.info("Got {} base info", size);
        EnterpriseQueue.recordNames(baseNames);

        // prepare all related company
        List<String> baseList = new ArrayList(baseNames);
        List<List> listGroup = new ArrayList<>();
        int index = 0;
        while (index * pageSize < size) {
            int length = (index + 1) * pageSize;
            int end = Math.min(size, length);
            List<String> subList = baseList.subList(index * pageSize, end);
            listGroup.add(subList);
            index++;
        }

        try {
            Integer num = pool.submit(new EnterpriseDetailsThread().setESService(esService).setNames(listGroup)).get();
            log.info("Got {} company info in total", num);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we handle baseInfo");
        }

        log.info("Handle {} data", EnterpriseCache.get().size());

        try (FileOutputStream outputStream = new FileOutputStream(new File(trgtUrl))) {
            List<String> titleItems = Arrays.asList("公司名称", "法人", "所属地区", "统一社会信用码", "注册日期", "核准日期", "公司类型", "经营状态", "登记机关", "注册资本", "注册地址", "经营范围", "路径");
            String title = String.join("|", titleItems).trim() + "\n";
            outputStream.write(title.getBytes());

            // prepare output data
            baseNames.stream().map(x -> {
                if("上海万企明道软件有限公司".equals(x)) {
                    log.info("Here");
                }
                Set<String> cache = new HashSet<>();
                List<String> data = new ArrayList<>();
                appendData(data, cache, x, "", 0, 0);
                return data;
            }).forEach(x -> x.forEach(y -> {
                try {
                    outputStream.write(y.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }));

        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when ");
        }
        return trgtUrl;
    }

    private void appendData(List dataList, Set<String> cache, String name, String perName, int deep, int position) {
        if (cache.add(name)) {
            Map baseInfo = EnterpriseCache.get(name);

            String path = name;
            if (position > 0) {
                path = perName + "->" + name;
            } else if (position < 0) {
                path = name + "->" + perName;
            }

//            String code = DataUtils.getNotNullValue(baseInfo, "province_code", String.class, "");
//            if (deep == 0 || (deep > 0 && "31".equals(code))) {
                String credit_code = DataUtils.getNotNullValue(baseInfo, "credit_code", String.class, "");
                List dataItems = Arrays.asList(
                          name
                        , DataUtils.getNotNullValue(baseInfo, "charge_person", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "province_name", String.class, "")
                        , (StringUtils.isNotBlank(credit_code)) ? "'" + credit_code : ""
                        , DataUtils.getNotNullValue(baseInfo, "establishment_date", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "approval_date", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "enterprises_type", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "registration_status", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "registration_authority", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "registered_capital_origin", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "residence", String.class, "")
                        , DataUtils.getNotNullValue(baseInfo, "business_scope", String.class, "")
                        , path + "\n"
                );

                if (position < 0) {
                    dataList.add(0, String.join("|", dataItems));
                } else {
                    dataList.add(String.join("|", dataItems));
                }
//            }

            String deepPath = path.trim();
            DataUtils.getNotNullValue(baseInfo, "shareholder_information", List.class, new ArrayList<>()).forEach(x -> appendData(dataList, cache, (String) x, deepPath, deep + 1, -1));
            EnterpriseCache.pop(name).forEach(x -> appendData(dataList, cache, (String) x, deepPath, deep + 1, 1));
        }
    }

    @PostConstruct
    void init() {
        // init static variables
        pageSize = LocalConfig.get(BusinessConstants.SysConfig.QUERY_PAGE_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);

        pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
