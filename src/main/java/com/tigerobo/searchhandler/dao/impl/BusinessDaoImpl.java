package com.tigerobo.searchhandler.dao.impl;

import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.dao.BusinessDao;
import com.tigerobo.searchhandler.entity.BusinessInformation;
import com.tigerobo.searchhandler.mapper.BusinessMapper;
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
        log.info("Try to load business info for param:[{}]", param);
        List<BusinessInformation> dataList = businessMapper.selectAllData(param);
        log.info("Got {} business info for param:[{}]", dataList.size(), param);
        return notNullList(dataList);
    }

    @Override
    public Long countForProvince(String provinceCode) {

        printSql(BusinessMapper.mapperPath + ".countAllData", null);
        log.info("Try to count business info for province:[{}]", provinceCode);
        Long dataNum = businessMapper.countAllData(provinceCode);
        log.info("Got {} business info for province:[{}]", dataNum, provinceCode);
        return DataUtils.handleNullValue(dataNum, Long.class, 0L);
    }
}
