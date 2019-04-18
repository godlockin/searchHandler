package searchhandler.service.impl;

import searchhandler.service.ESService;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import searchhandler.exception.SearchHandlerException;
import searchhandler.service.SearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Slf4j
@Service
public class SearchServiceImpl implements SearchService {

    @Autowired
    private ESService esService;

    @Override
    public Map doSimpleSearch(Map param) throws SearchHandlerException {

        log.debug("Do simple search for param:[{}]", param);
        Map result = esService.simpleSearch(param);
        log.debug("Got result:[{}]", result);
        return result;
    }

    @Override
    public Map doComplexSearch(Map param) throws SearchHandlerException {

        log.debug("Do complex search for param:[{}]", param);
        Map result = esService.complexSearch(param);
        log.debug("Got result:[{}]", result);
        return result;
    }

    @Override
    public Map doAnalyze(Map param) throws SearchHandlerException {

        log.debug("Do analyze for param:[{}]", param);
        Map result = new HashMap();

        List<String> textList = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.QUERY_KEY, List.class, new ArrayList<>());
        if (textList.isEmpty()) {
            log.error("No trgt text found");
            return result;
        }

//        String fullTxt = "";
//        textList.stream().map(String::trim).filter(StringUtils::isNotBlank).reduce(fullTxt, (full, tmp) -> full += tmp);
//        String analyzer = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.ANALYZER_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_ANALYZER);
//        String wordsStr = RedisUtil.hget(analyzer, fullTxt);
//        if (StringUtils.isNotBlank(wordsStr)) {
//            List terms = Arrays.asList(wordsStr.split(BusinessConstants.DELIMITER));
//            result.put(BusinessConstants.ResultConfig.DATA_KEY, terms);
//        } else {
            result = esService.doAnalyze(param);
//        }
        log.debug("Got result:[{}]", result);
        return result;
    }
}
