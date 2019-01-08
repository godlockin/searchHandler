package com.searchhandler.common.component;

import com.searchhandler.dao.BusinessDao;
import com.searchhandler.model.KeyPesonModel;
import com.searchhandler.service.ESService;
import com.searchhandler.common.constants.BusinessConstants;
import com.searchhandler.common.constants.ResultEnum;
import com.searchhandler.common.utils.DataUtils;
import com.searchhandler.entity.BusinessInformation;
import com.searchhandler.exception.SearchHandlerException;
import com.searchhandler.model.BusinessModel;
import com.searchhandler.model.ShareHolderModel;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@Data
@Slf4j
@Component
public class DataTransportCallable implements Callable<Long> {

    private Map<String, Object> config;
    private ESService esService;
    private BusinessDao businessDao;

    private String esType;
    private String esIndex;
    private Integer pageSize;

    private String provinceCode;
    private String provinceName;

    public DataTransportCallable() {}

    public DataTransportCallable(Map<String, Object> config, ESService esService, BusinessDao businessDao) {

        this.config = config;
        this.esService = esService;
        this.businessDao = businessDao;

        this.provinceCode = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, String.class, "");
        this.provinceName = BusinessConstants.PROVINCE_MAP.get(provinceCode);
        this.pageSize = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_PAGE_SIZE, Integer.class, BusinessConstants.QueryConfig.DEFAULT_PAGE_SIZE);
        this.esIndex = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESINDEX, String.class, "");
        this.esType = DataUtils.getNotNullValue(config, BusinessConstants.QueryConfig.KEY_ESTYPE, String.class, "");
    }

    @Override
    public Long call() throws Exception {

        log.info("Try to load data for [{}]-[{}] and insert [{}]-[{}]", provinceCode, provinceName, esIndex, esType);
        try {
            long count = 0L;
            if (StringUtils.isBlank(provinceCode)) {
                log.error("Important info missing");
                return count;
            }

            List<Map<String, Object>> bulkList = new ArrayList<>();
            // load target data
            Map<String, Object> param = new HashMap<>(this.config);
            List<Long> idList = businessDao.findTargetPk(param);
            if (CollectionUtils.isEmpty(idList)) {
                log.error("No target id found");
                return count;
            }

            param.put(BusinessConstants.QueryConfig.KEY_QUERY_IDLIST, idList);
            List<BusinessInformation> dataList = businessDao.searchForProvince(param);
            int dataSize = dataList.size();
            log.info("Get {} data to be handled", dataSize);
            boolean isInit = true;
            Set<Long> baseCorrelationNoSet = new HashSet<>();
            Set<Long> baseKSerialNoSet = new HashSet<>();
            Set<Long> baseSSerialNoSet = new HashSet<>();
            BusinessModel model = new BusinessModel(provinceCode, provinceName);
            for (BusinessInformation bi : dataList) {
                // if switch base data, save &/| commit
                if (baseCorrelationNoSet.add(bi.getCorrelationNo())) {
                    if (!isInit) {
                        bulkList.add(convertModelToMap(model));
                        log.debug("Save data provinceCode:[{}]-[{}] serialno:[{}], correlationNo:[{}]",
                                provinceCode, provinceName, model.getSerialno(), model.getCorrelation_no());

                        // commit & refresh tmp list
                        if (bulkList.size() > pageSize) {
                            count += esService.bulkInsert(esIndex, esType, "id", bulkList);
                            bulkList = new ArrayList<>();
                        }
                    }
                    model = new BusinessModel(provinceCode, provinceName).initEssentialData(bi);
                    isInit = false;
                }

                // append new key person
                if (baseKSerialNoSet.add(bi.getKSerialno())) {
                    model.getKey_personnel().add(new KeyPesonModel(bi));
                }

                // append new share holder
                if (baseSSerialNoSet.add(bi.getSSerialno())) {
                    model.getShareholder_information().add(new ShareHolderModel(bi));
                }
            }

            // to save the last company if isn't been committed
            bulkList.add(convertModelToMap(model));

            count += esService.bulkInsert(esIndex, esType, "id", bulkList);
            log.info("Finished handled {} data", count);
            return count;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Error happened when we load data for [%s]-[%s] and insert [%s]-[%s], %s",
                    provinceCode, provinceName, esIndex, esType, e);
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_BULK_COMMIT, errMsg);
        }
    }

    private Map<String, Object> convertModelToMap(BusinessModel model) {
        // essential info
        Map data = new HashMap(){{
            put("id", String.valueOf(model.getCorrelation_no()));
            put("province_code", model.getProvince_code());
            put("province_name", model.getProvince_name());
            put("serialno", model.getSerialno());
            put("correlation_no", model.getCorrelation_no());
            put("registration_no", model.getRegistration_no());
            put("credit_code", model.getCredit_code());
            put("enterprises_type", model.getEnterprises_type());
            put("registered_capital_origin", model.getRegistered_capital_origin());
            put("business_time_start", model.getBusiness_time_start());
            put("registration_authority", model.getRegistration_authority());
            put("registration_status", model.getRegistration_status());
            put("residence", model.getResidence());
            put("business_scope", model.getBusiness_scope());
            put("enterprise_name", model.getEnterprise_name());
            put("charge_person", model.getCharge_person());
            put("establishment_date", model.getEstablishment_date());
            put("business_time_end", model.getBusiness_time_end());
            put("approval_date", model.getApproval_date());
            put("create_time", model.getCreate_time());
            put("spider_time", model.getSpider_time());
        }};

        // key personal info
        data.put("key_personnel", model.getKey_personnel().parallelStream().filter(Objects::nonNull).map(x -> new HashMap() {
            {
                put("serialno", x.getSerialno());
                put("name", x.getName());
                put("position_chinese", x.getPosition_chinese());
                put("create_time", x.getCreate_time());
            }
        }).collect(Collectors.toList()));

        // share holder info
        data.put("shareholder_information", model.getShareholder_information().parallelStream().filter(Objects::nonNull).map(x -> new HashMap() {
            {
                put("serialno", x.getSerialno());
                put("shareholder_name", x.getShareholder_name());
                put("shareholder_type", x.getShareholder_type());
                put("payable_amount", x.getPayable_amount());
                put("payable_actual_amount", x.getPayable_actual_amount());
                put("shareholder_credit_type", x.getShareholder_credit_type());
                put("shareholder_credit_no", x.getShareholder_credit_no());
                put("create_time", x.getCreate_time());
            }
        }).collect(Collectors.toList()));
        return data;
    }
}
