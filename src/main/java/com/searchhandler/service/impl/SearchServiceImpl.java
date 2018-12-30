package com.searchhandler.service.impl;

import com.searchhandler.service.ESService;
import com.searchhandler.service.SearchService;
import com.searchhandler.exception.SearchHandlerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

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
        Map result = esService.doAnalyze(param);
        log.debug("Got result:[{}]", result);
        return result;
    }
}
