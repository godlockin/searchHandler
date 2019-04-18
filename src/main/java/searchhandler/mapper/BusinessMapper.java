package searchhandler.mapper;

import searchhandler.entity.BusinessInformation;
import org.apache.ibatis.annotations.Mapper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Mapper
@Service
public interface BusinessMapper {

    String mapperPath = "searchhandler.mapper.BusinessMapper";

    List<BusinessInformation> selectAllData(Map param);

    Long countAllData(Map config);

    List<Long> findTargetPk(Map param);

    List<BusinessInformation> queryForCNo(Map param);
}
