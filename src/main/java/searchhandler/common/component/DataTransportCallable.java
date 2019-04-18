package searchhandler.common.component;

import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.constants.BusinessConstants.QueryConfig;
import searchhandler.common.constants.ResultEnum;
import searchhandler.common.utils.DataUtils;
import searchhandler.dao.BusinessDao;
import searchhandler.entity.BusinessInformation;
import searchhandler.exception.SearchHandlerException;
import searchhandler.service.DataTransportService;
import searchhandler.service.ESService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Data
@Slf4j
@Component
public class DataTransportCallable implements Callable<Long> {

    private Map<String, Object> config;
    private ESService esService;
    private BusinessDao businessDao;
    private DataTransportService dataTransportService;

    private String esType;
    private String esIndex;

    private String provinceCode;
    private String provinceName;

    public DataTransportCallable() {}

    public DataTransportCallable(Map<String, Object> config, ESService esService, BusinessDao businessDao, DataTransportService dataTransportService) {

        this.config = config;
        this.esService = esService;
        this.businessDao = businessDao;
        this.dataTransportService = dataTransportService;

        this.provinceCode = DataUtils.getNotNullValue(config, QueryConfig.KEY_PROVINCE_CODE, String.class, "");
        this.provinceName = BusinessConstants.PROVINCE_MAP.get(provinceCode);
        this.esIndex = DataUtils.getNotNullValue(config, QueryConfig.KEY_ESINDEX, String.class, "");
        this.esType = DataUtils.getNotNullValue(config, QueryConfig.KEY_ESTYPE, String.class, "");
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

            // load target data
            Map<String, Object> param = new HashMap<>(this.config);
            List<Long> idList = businessDao.findTargetPk(param);
            if (CollectionUtils.isEmpty(idList)) {
                log.error("No target id found");
                return count;
            }

            param.put(QueryConfig.KEY_QUERY_IDLIST, idList);
            List<BusinessInformation> dataList = businessDao.searchForProvince(param);

            Map insertParam = new HashMap() {{
                put(QueryConfig.KEY_ESINDEX, esIndex);
                put(QueryConfig.KEY_ESTYPE, esType);
                put(QueryConfig.KEY_PROVINCE_CODE, provinceCode);
                put(QueryConfig.KEY_PROVINCE_NAME, provinceName);
                put(BusinessConstants.DataDumpConfig.DATA_LIST_KEY, dataList);
            }};

            count += dataTransportService.handleBaseDataList(insertParam);
            return count;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = String.format("Error happened when we load data for [%s]-[%s] and insert [%s]-[%s], %s",
                    provinceCode, provinceName, esIndex, esType, e);
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_BULK_COMMIT, errMsg);
        }
    }
}
