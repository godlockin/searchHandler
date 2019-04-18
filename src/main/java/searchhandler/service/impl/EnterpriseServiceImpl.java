package searchhandler.service.impl;

import searchhandler.common.LocalConfig;
import searchhandler.common.component.EnterpriseDetailsCallable;
import searchhandler.service.ESService;
import searchhandler.common.EnterpriseCache;
import searchhandler.common.EnterpriseQueue;
import searchhandler.common.component.FullCacheCallable;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.constants.ResultEnum;
import searchhandler.common.utils.DataUtils;
import searchhandler.common.utils.RedisUtil;
import searchhandler.exception.SearchHandlerException;
import searchhandler.service.EnterpriseService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class EnterpriseServiceImpl implements EnterpriseService {

    @Autowired
    private ESService esService;
    private Integer pageSize;
    private ExecutorService pool;

    @Override
    public String generateReportOnEnterprise(Map param) throws SearchHandlerException {


        String baseUrl = DataUtils.getNotNullValue(param, "baseUrl", String.class, "");
        String trgtUrl = DataUtils.getNotNullValue(param, "trgtUrl", String.class, "");

        trgtUrl = (StringUtils.isBlank(trgtUrl)) ? baseUrl + "_output.csv" : trgtUrl;
        log.info("Try to build info from:[{}] and write into:[{}]", baseUrl, trgtUrl);

        // prepare base companies
        String errMsg;
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(baseUrl)))){
            String tmp;
            while ((tmp = reader.readLine()) != null) {
                tmp = tmp.replace(" ", "").trim();
                tmp = DataUtils.fullWidth2halfWidth(tmp);
                EnterpriseQueue.recordBaseNames(tmp);
            }
            EnterpriseQueue.resetRecordNames();
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
            long num = pool.submit(new EnterpriseDetailsCallable().setNames(listGroup).setParam(param).setESService(esService)).get();
            log.info("Got {} company info in total", num);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we handle baseInfo");
        }

        log.info("Handle {} data", EnterpriseCache.get().size());

        try (FileOutputStream outputStream = new FileOutputStream(new File(trgtUrl))) {
            List titleItems = Arrays.asList("公司名称", "法人", "所属地区", "统一社会信用码", "注册日期", "核准日期", "公司类型", "经营状态", "登记机关", "注册资本", "注册地址", "经营范围", "路径");
            String title = String.join("|", titleItems).trim() + "\n";
            outputStream.write(title.getBytes());

            // prepare output data
            baseNames.stream().map(x -> {
                Set<String> cache = new HashSet<>();
                List<String> data = new ArrayList();
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
            log.error("Error happened when we generate enterprise report");
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
            if (deep <= 2) {
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
            } else {
                return;
            }

            String deepPath = path.trim();
            List shareholder_information = DataUtils.getNotNullValue(baseInfo, "shareholder_information", List.class, new ArrayList<>());
            shareholder_information.forEach(x -> appendData(dataList, cache, (String) x, deepPath, deep + 1, -1));

            List shareHolders = EnterpriseCache.pop(name);
            shareHolders.forEach(x -> appendData(dataList, cache, (String) x, deepPath, deep + 1, 1));
        }
    }

    @Override
    public Long generateEquity(Map param) throws SearchHandlerException {
        log.info("Start to generate equity presentation");
        long count;
        try {
            count = pool.submit(new FullCacheCallable().setParam(new HashMap(param)).setESService(esService)).get();
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to cache enterprise info" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_QUERY, errMsg);
        }

        log.info("Cached {} data", count);

//        String index = DataUtils.getNotNullValue(param, BusinessConstants.BusinessDataConfig.EQUITY_INDEX_KEY, String.class, BusinessConstants.BusinessDataConfig.DEFAULT_EQUITY_INDEX);
//        String type = DataUtils.getNotNullValue(param, BusinessConstants.BusinessDataConfig.EQUITY_TYPE_KEY, String.class, BusinessConstants.BusinessDataConfig.DEFAULT_EQUITY_TYPE);
//
//        log.info("Start to build equity info");
//        String guid = EnterpriseQueue.poll();
//        while (StringUtils.isNotBlank(guid)) {
//            if (RedisUtil.exists(guid)) {
//                Set<String> distinct = Sets.newConcurrentHashSet();
//                Map data = buildEquity(distinct, 0).apply(RedisUtil.hgetAll(guid));
//                esService.bulkInsert(index, type, "guid", data);
//            }
//            guid = EnterpriseQueue.poll();
//        }

        log.info("Finished equity info's build");
        return count;
    }

    private Function<Map, Map> buildEquity(Set<String> distinct, final int level) {
        return data -> {
            String guid = DataUtils.getNotNullValue(data, "guid", String.class, "");
            data.put("level", level);
            int nextLevel = level + 1;
            if (distinct.add(guid) && nextLevel < 3) {
                data.put("shareholder", handleDeeperNode(guid + "_core", distinct, nextLevel));
                data.put("investment", handleDeeperNode(guid + "_si", distinct, nextLevel));
            }
            return data;
        };
    }

    private List handleDeeperNode(String key, Set<String> distinct, int currLevel) {
        List result = Optional.ofNullable(RedisUtil.smembers(key)).orElse(new HashSet<>())
                .stream()
                .map(RedisUtil::hgetAll)
                .filter(x -> !(null == x || x.isEmpty()))
                .map(buildEquity(distinct, currLevel)::apply)
                .collect(Collectors.toList());
        return (result.isEmpty()) ? new ArrayList() : result;
    }

    @PostConstruct
    void initPool() {
        pageSize = LocalConfig.get(BusinessConstants.SysConfig.QUERY_PAGE_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        pool = new ThreadPoolExecutor(1024, 1024, 10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(Runtime.getRuntime().availableProcessors() * 2),
                new ThreadPoolExecutor.AbortPolicy());
    }
}
