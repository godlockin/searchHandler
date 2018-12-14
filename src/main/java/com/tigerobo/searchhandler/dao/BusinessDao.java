package com.tigerobo.searchhandler.dao;

import com.tigerobo.searchhandler.entity.BusinessInformation;

import java.util.List;
import java.util.Map;

public interface BusinessDao {

    List<BusinessInformation> searchForProvince(Map param);
    Long countForProvince(String provinceCode);
    List<Long> findTargetPk(Map param);
}
