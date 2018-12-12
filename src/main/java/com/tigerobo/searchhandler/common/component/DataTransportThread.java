package com.tigerobo.searchhandler.common.component;

import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.dao.BusinessDao;
import com.tigerobo.searchhandler.entity.BusinessInformation;
import com.tigerobo.searchhandler.model.BusinessModel;
import com.tigerobo.searchhandler.model.KeyPesonModel;
import com.tigerobo.searchhandler.model.ShareHolderModel;
import com.tigerobo.searchhandler.service.ESService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
@Slf4j
@Component
public class DataTransportThread extends Thread {

    private String esType;
    private String esIndex;
    private Integer pageSize;
    private ESService esService;
    private BusinessDao businessDao;

    private Long index = 0L;
    private Integer size = 0;
    private String provinceCode;
    private String provinceName;

    @Override
    public void run() {

        log.info("Try to load data for [{}]-[{}] and insert [{}]-[{}]", provinceCode, provinceName, esIndex, esType);
        try {
            if (StringUtils.isBlank(provinceCode) || StringUtils.isBlank(esIndex) || StringUtils.isBlank(esType)) {
                log.error("Important info missing");
                return;
            }

            long count = 0L;
            List bulkList = new ArrayList();
            // load target data
            Map param = new HashMap();
            param.put("provinceCode", provinceCode);
            if (size != 0) {
                param.put("index", index);
                param.put("size", size);
            }

            List<BusinessInformation> dataList = businessDao.searchForProvince(param);
            int dataSize = dataList.size();
            log.info("Get {} data to be handled", dataSize);
            Long baseCorrelationNo = 0L;
            int baseKSerialNo = 0;
            int baseSSerialNo = 0;
            BusinessModel model = new BusinessModel(provinceCode, provinceName);
            for (int i = 0; i < dataSize; i++) {
                BusinessInformation bi = dataList.get(i);
                long correlationNo = bi.getCorrelationNo();
                // if switch base data, save &/| commit
                if (correlationNo != baseCorrelationNo) {
                    if (0 != baseCorrelationNo) {
                        bulkList.add(convertModelToMap(model));
                        log.info("Save data provinceCode:[{}]-[{}] serialno:[{}], correlationNo:[{}]", provinceCode, provinceName, model.getSerialno(), model.getCorrelation_no());

                        // commit & refresh tmp list
                        if (bulkList.size() > pageSize) {
                            count += esService.bulkInsert(esIndex, esType, "id", bulkList);
                            bulkList = new ArrayList();
                        }
                    }
                    model = new BusinessModel(provinceCode, provinceName);
                    model.initEssentialData(bi);
                    baseCorrelationNo = correlationNo;
                }

                // append new key person
                if (bi.getKSerialno() != baseKSerialNo) {
                    model.getKey_personnel().add(new KeyPesonModel(bi));
                    baseKSerialNo = bi.getKSerialno();
                }

                // append new share holder
                if (bi.getSSerialno() != baseSSerialNo) {
                    model.getShareholder_information().add(new ShareHolderModel(bi));
                    baseSSerialNo = bi.getSSerialno();
                }
            }

            count += esService.bulkInsert(esIndex, esType, "id", bulkList);
            log.info("Finished handled {} data", count);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we load data for [{}]-[{}] and insert [{}]-[{}], {}",
                    provinceCode, provinceName, esIndex, esType, e);
        }
    }

    private Map convertModelToMap(BusinessModel model) {
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
        data.put("key_personnel", model.getKey_personnel().stream().map(x -> new HashMap() {
            {
                put("serialno", x.getSerialno());
                put("name", x.getName());
                put("position_chinese", x.getPosition_chinese());
                put("create_time", x.getCreate_time());
            }
        }).collect(Collectors.toList()));

        // share holder info
        data.put("shareholder_information", model.getShareholder_information().stream().map(x -> new HashMap() {
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
