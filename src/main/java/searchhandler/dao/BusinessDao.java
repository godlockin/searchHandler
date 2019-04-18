package searchhandler.dao;

import searchhandler.entity.BusinessInformation;

import java.util.List;
import java.util.Map;

public interface BusinessDao {

    List<BusinessInformation> searchForProvince(Map param);
    Long countForProvince(Map config);
    List<Long> findTargetPk(Map param);

    List<BusinessInformation> searchForCNo(Map param);
}
