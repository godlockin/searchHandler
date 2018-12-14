package com.tigerobo.searchhandler.mapper;

import com.tigerobo.searchhandler.entity.BusinessInformation;
import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

public interface BusinessMapper {

    String mapperPath = "com.tigerobo.searchhandler.mapper.BusinessMapper";

    List<BusinessInformation> selectAllData(Map param);

    Long countAllData(@Param(value = "provinceCode") String provinceCode);

    List<Long> findTargetPk(Map param);
}
