package com.searchhandler.dao.impl;

import com.searchhandler.dao.BusinessDao;
import com.searchhandler.common.utils.DataUtils;
import com.searchhandler.entity.BusinessInformation;
import com.searchhandler.mapper.BusinessMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class BusinessDaoImpl extends BaseDaoImpl implements BusinessDao {

    @Autowired
    private BusinessMapper businessMapper;

    @Override
    public List<BusinessInformation> searchForProvince(Map param) {

        printSql(BusinessMapper.mapperPath + ".selectAllData", param);
        String provinceCode = DataUtils.getNotNullValue(param, "provinceCode", String.class, "");
        Long index = DataUtils.getNotNullValue(param, "index", Long.class, 0L);
        Integer size = DataUtils.getNotNullValue(param, "size", Integer.class, 0);
        log.info("Try to load business info for provinceCode:[{}], limit:[{}] size:[{}]", provinceCode, index, size);
        List<BusinessInformation> dataList = businessMapper.selectAllData(param);
        log.info("Got {} business info for provinceCode:[{}], limit:[{}] size:[{}]", dataList.size(), provinceCode, index, size);
        return notNullList(dataList);
    }

    @Override
    public Long countForProvince(Map config) {

        printSql(BusinessMapper.mapperPath + ".countAllData", config);
        log.info("Try to count business info for config:[{}]", config);
        Long dataNum = businessMapper.countAllData(config);
        log.info("Got {} business info for config:[{}]", dataNum, config);
        return DataUtils.handleNullValue(dataNum, Long.class, 0L);
    }

    @Override
    public List<Long> findTargetPk(Map param) {

        printSql(BusinessMapper.mapperPath + ".findTargetPk", param);
        log.info("Try to find target serial no for:[{}]", param);
        List<Long> idList = businessMapper.findTargetPk(param);
        log.info("Got {} ids for param:[{}]", idList.size(), param);
        return notNullList(idList);
    }
}
