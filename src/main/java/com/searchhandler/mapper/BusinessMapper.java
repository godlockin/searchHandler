package com.searchhandler.mapper;

import com.searchhandler.entity.BusinessInformation;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component(value = "businessMapper")
public interface BusinessMapper {

    String mapperPath = "BusinessMapper";

    List<BusinessInformation> selectAllData(Map param);

    Long countAllData(Map config);

    List<Long> findTargetPk(Map param);
}
