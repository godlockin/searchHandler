package searchhandler.service.impl;

import com.alibaba.fastjson.JSON;
import searchhandler.common.LocalConfig;
import searchhandler.common.component.DataFixMonitorThread;
import searchhandler.common.component.DataTransportMonitorThread;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.GuidService;
import searchhandler.common.utils.RedisUtil;
import searchhandler.entity.BusinessInformation;
import searchhandler.model.BusinessModel;
import searchhandler.model.KeyPesonModel;
import searchhandler.model.ShareHolderModel;
import searchhandler.service.DataTransportService;
import searchhandler.service.ESService;
import searchhandler.common.LocalCache;
import searchhandler.common.utils.DataUtils;
import searchhandler.dao.BusinessDao;
import searchhandler.exception.SearchHandlerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class DataTransportServiceImpl implements DataTransportService {

    private Integer pageSize;
    private Integer threadNum;

    @Autowired
    private ESService esService;
    @Autowired
    private BusinessDao businessDao;
    private DataFixMonitorThread dataFixMonitorThread;

    @Override
    public Long coreDump(Map<String, Object> config) {
        Long count = 0L;
        log.info("Try to do dump for:[{}]", JSON.toJSONString(config));
        String provinceCode = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, "");
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
        param.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, BusinessConstants.QueryConfig.DEFAULT_QUERY_INDEX);
        param.put(BusinessConstants.QueryConfig.KEY_PAGE_SIZE, pageSize);
        log.info("Got query param:[{}]", param);

        // over size, we need to paging the queries
        if (pageSize < count) {
            long pageCount = count / pageSize;
            for (long index = 0; index < pageCount + 1; index++) {
                // set the query from index
                param.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, index * pageSize);
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
    public Long dailyDump(Map param) {
        log.info("Try to full dump data");
        long count = doDumpData(doIncrementalDump(param));
        log.info("Handled {} data in total", count);
        return count;
    }

    @Override
    public Long reindex(Map param) throws SearchHandlerException {
        log.info("Try to reindex data");
        Map query = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY, Map.class, new HashMap<>());
        query.put(BusinessConstants.QueryConfig.KEY_QUERY_SIZE, 0);
        Map result = esService.complexSearch(query);
        Long total = DataUtils.getNotNullValue(result, BusinessConstants.ResultConfig.TOTAL_KEY, Long.class, 0L);
        log.info("Will handled {} data in total", total);
        if (null == dataFixMonitorThread) {
            dataFixMonitorThread = new DataFixMonitorThread(param, esService);
            dataFixMonitorThread.start();
        } else {
            log.warn("Another reindex job is running");
        }

        return total;
    }


    private Function<String, Long> dataDumpForCNo(Map m) {

        return (provinceCode) -> {
            Long count = 0L;
            String key = "province_" + provinceCode;
            boolean isRead = true;
            boolean isSaved = false;
            List<Long> ids = new ArrayList<>();
            do {
                isSaved = false;
                String msg = RedisUtil.lpop(key);
                if (StringUtils.isNotBlank(msg)) {
                    Long id = Long.valueOf(msg);
                    ids.add(id);

                    if (ids.size() >= pageSize) {

                        List trgtId = new ArrayList(ids);
                        count += dataFix(new HashMap() {{
                            put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);
                            put(BusinessConstants.DataDumpConfig.DATA_LIST_KEY, trgtId);
                        }});

                        ids = new ArrayList<>();
                        isSaved = true;
                    }
                } else {
                    isRead = false;
                }

            } while (isRead);

            if (!isSaved) {
                List trgtId = new ArrayList(ids);
                count += dataFix(new HashMap() {{
                    put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);
                    put(BusinessConstants.DataDumpConfig.DATA_LIST_KEY, trgtId);
                }});
            }

            return count;
        };
    }

    @Override
    public Long dataDumpForCNo() {

        log.info("Try to full dump data");
        long count = doDumpData(dataDumpForCNo(new HashMap()));
        log.info("Handled {} data in total", count);
        return count;
    }

    @Override
    public Long dataFix(Map param) {
        log.info("Try to fix data");
        long count = 0L;
        String provinceCode = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, "");
        if (StringUtils.isBlank(provinceCode) || !BusinessConstants.PROVINCE_MAP.keySet().contains(provinceCode)) {
            log.error("Illegal provinceCode");
            return count;
        }

        List<BusinessInformation> dataList = new ArrayList<>();

        // fix for
        List correlationNo = DataUtils.getNotNullValue(param, BusinessConstants.DataDumpConfig.DATA_LIST_KEY, List.class, new ArrayList<>());
        if (!correlationNo.isEmpty()) {
            dataList = businessDao.searchForCNo(param);
        }

        if (dataList.isEmpty()) {
            log.error("No trgt record found");
            return count;
        }

        Map insertParam = new HashMap();
        insertParam.put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);
        insertParam.put(BusinessConstants.DataDumpConfig.DATA_LIST_KEY, dataList);

        count += handleBaseDataList(insertParam);

        log.info("Handled {} data in total", count);
        return count;
    }

    @Override
    public Long fullDumpData(Map param) {
        log.info("Try to full dump data");
        long count = doDumpData(doFullDump(param));
        log.info("Handled {} data in total", count);
        return count;
    }

    private Long doDumpData(Function<String, Long> function) {
        log.info("Try to dump data");
        long count = BusinessConstants.PROVINCE_MAP.keySet().parallelStream()
                .collect(Collectors.summarizingLong(function::apply)).getSum();
        log.info("Handled {} data in total", count);
        return count;
    }

    @Override
    public long handleBaseDataList(Map config) {

        long count = 0L;
        String esIndex = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESINDEX, String.class, BusinessConstants.ESConfig.DEFAULT_ES_INDEX);
        String esType = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESTYPE, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE);

        String provinceCode = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, "");
        String provinceName = BusinessConstants.PROVINCE_MAP.get(provinceCode);
        List<BusinessInformation> dataList = DataUtils.getNotNullValue(config, BusinessConstants.DataDumpConfig.DATA_LIST_KEY, List.class, new ArrayList<>());

        if (!BusinessConstants.PROVINCE_MAP.keySet().contains(provinceCode) || dataList.isEmpty()) {
            return count;
        }

        List<Map<String, Object>> bulkList = new ArrayList<>();
        int dataSize = dataList.size();
        log.info("Get {} data to be handled", dataSize);
        Set<Long> baseSet = new HashSet<>();
        Set<Long> kpSet = new HashSet<>();
        Set<Long> siSet = new HashSet<>();
        boolean isInit = true;
        boolean isSaved = true;
        BusinessModel model = new BusinessModel(provinceCode, provinceName);
        for (BusinessInformation bi : dataList) {
            isSaved = false;

            if (isIllegalData(bi)) {
                continue;
            }

            // if switch base   data, save &/| commit
            if (baseSet.add(bi.getCorrelationNo())) {
                if (!isInit) {
                    bulkList.add(convertModelToMap(model));
                    log.debug("Save data provinceCode:[{}]-[{}] serialno:[{}], correlationNo:[{}]",
                            provinceCode, provinceName, model.getSerialno(), model.getCorrelation_no());

                    // commit & refresh tmp list
                    if (bulkList.size() > pageSize) {
                        count += esService.bulkInsert(esIndex, esType, "id", bulkList);
                        bulkList = new ArrayList<>();
                        isSaved = true;
                    }
                }
                model = new BusinessModel(provinceCode, provinceName).initEssentialData(bi);
                isInit = false;
            }

            // append new key person
            if (kpSet.add(bi.getKSerialno())) {
                model.getKey_personnel().add(new KeyPesonModel(bi));
            }

            // append new share holder
            if (siSet.add(bi.getSSerialno())) {
                model.getShareholder_information().add(new ShareHolderModel(bi));
            }
        }

        // to save the last company if isn't been committed
        if (!isSaved) {
            bulkList.add(convertModelToMap(model));
        }

        count = esService.bulkInsert(esIndex, esType, "id", bulkList);
        log.info("Finished handled {} data", count);
        return count;
    }

    private boolean isIllegalData(BusinessInformation bi) {
        return isIllegalLong(bi.getCorrelationNo()) ||
                (StringUtils.isBlank(bi.getEnterpriseName())) ||
                isIllegalLong(bi.getSerialno()) ||
                isIllegalLong(bi.getKSerialno()) ||
                isIllegalLong(bi.getSSerialno());
    }

    private boolean isIllegalLong(Long num) {
        return 0 >= Optional.ofNullable(num).orElse(-1L);
    }

    private Map<String, Object> convertModelToMap(BusinessModel model) {
        // essential info
        String enterprise_name = DataUtils.fullWidth2halfWidth(model.getEnterprise_name());
        Map data = new HashMap(){{
            put("id", String.valueOf(model.getCorrelation_no()));
            put("guid", GuidService.getGuid(enterprise_name));
            put("province_code", model.getProvince_code());
            put("province_name", DataUtils.fullWidth2halfWidth(model.getProvince_name()));
            put("serialno", model.getSerialno());
            put("correlation_no", model.getCorrelation_no());
            put("registration_no", model.getRegistration_no());
            put("credit_code", model.getCredit_code());
            put("enterprises_type", DataUtils.fullWidth2halfWidth(model.getEnterprises_type()));
            put("registered_capital_origin", model.getRegistered_capital_origin());
            put("business_time_start", model.getBusiness_time_start());
            put("registration_authority", DataUtils.fullWidth2halfWidth(model.getRegistration_authority()));
            put("registration_status", DataUtils.fullWidth2halfWidth(model.getRegistration_status()));
            put("residence", DataUtils.fullWidth2halfWidth(model.getResidence()));
            put("business_scope", DataUtils.fullWidth2halfWidth(model.getBusiness_scope()));
            put("enterprise_name", enterprise_name);
            put("charge_person", DataUtils.fullWidth2halfWidth(model.getCharge_person()));
            put("establishment_date", model.getEstablishment_date());
            put("business_time_end", model.getBusiness_time_end());
            put("approval_date", model.getApproval_date());
            put("create_time", model.getCreate_time());
            put("spider_time", model.getSpider_time());
        }};

        // key personal info
        Set<Long> kpSet = new HashSet<>();
        data.put("key_personnel", model.getKey_personnel().parallelStream().filter(Objects::nonNull)
                .filter(x -> distinct(kpSet).apply(x.getSerialno())).map(x -> new HashMap() {{
                    put("serialno", x.getSerialno());
                    put("name", DataUtils.fullWidth2halfWidth(x.getName()));
                    put("position_chinese", DataUtils.fullWidth2halfWidth(x.getPosition_chinese()));
                    put("create_time", x.getCreate_time());
                }}).collect(Collectors.toList()));

        // share holder info
        Set<Long> siSet = new HashSet<>();
        data.put("shareholder_information", model.getShareholder_information().parallelStream().filter(Objects::nonNull)
                .filter(x -> distinct(siSet).apply(x.getSerialno())).map(x -> new HashMap() {{
                    put("serialno", x.getSerialno());
                    put("shareholder_name", DataUtils.fullWidth2halfWidth(x.getShareholder_name()));
                    put("shareholder_type", DataUtils.fullWidth2halfWidth(x.getShareholder_type()));
                    put("payable_amount", x.getPayable_amount());
                    put("payable_actual_amount", x.getPayable_actual_amount());
                    put("shareholder_credit_type", DataUtils.fullWidth2halfWidth(x.getShareholder_credit_type()));
                    put("shareholder_credit_no", x.getShareholder_credit_no());
                    put("create_time", x.getCreate_time());
                }}).collect(Collectors.toList()));
        return data;
    }

    private Function<Long, Boolean> distinct(Set<Long> set) {
        return base -> !(null == base || 0L == base) && set.add(base);
    }

    private Function<String, Long> doFullDump(Map param) {

        return (provinceCode) -> coreDump(new HashMap(param){{put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);}});
    }

    private Function<String, Long> doIncrementalDump(Map param) {

        return (provinceCode) -> {
            Map<String, Object> serialNoInfo = new HashMap(param) {{put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);}};
            Map queryParam = buildDailyAggParams(provinceCode);
            log.debug("Get agg query for:[{}]-[{}] as:[{}]", provinceCode,
                    BusinessConstants.PROVINCE_MAP.get(provinceCode), JSON.toJSONString(queryParam));
            queryParam.put(BusinessConstants.QueryConfig.KEY_QUERY_INDEX, param.get(BusinessConstants.QueryConfig.KEY_ESINDEX));
            queryParam.put(BusinessConstants.QueryConfig.KEY_QUERY_TYPE, param.get(BusinessConstants.QueryConfig.KEY_ESTYPE));

            try {
                Map result = esService.complexSearch(queryParam);
                Map agg = DataUtils.getNotNullValue(result, BusinessConstants.ESConfig.AGGREGATION_KEY, Map.class, new HashMap<>());
                serialNoInfo.put(BusinessConstants.DataDumpConfig.ESSENTIAL_INFORMATION_KEY,
                        Double.valueOf((String) agg.get("essential_information.value")).longValue()
                );
                serialNoInfo.put(BusinessConstants.DataDumpConfig.KEY_PERSONNEL_KEY,
                        Double.valueOf((String) agg.get("key_personnel.kps_max.value")).longValue()
                );
                serialNoInfo.put(BusinessConstants.DataDumpConfig.SHAREHOLDER_INFORMATION_KEY,
                        Double.valueOf((String) agg.get("shareholder_information.sis_max.value")).longValue()
                );

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
        pageSize = LocalConfig.get(BusinessConstants.SysConfig.QUERY_PAGE_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        threadNum = LocalConfig.get(BusinessConstants.SysConfig.THREAD_POOL_SIZE_KEY, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);

        // init a monitor thread to provide & consume the query tasks
        DataTransportMonitorThread monitorThread = new DataTransportMonitorThread();
        monitorThread.setDataTransportService(this);
        monitorThread.setBusinessDao(businessDao);
        monitorThread.setEsService(esService);
        monitorThread.setThreadNum(threadNum);
        monitorThread.start();
    }
}
