package com.tigerobo.searchhandler.service.impl;

import com.tigerobo.searchhandler.exception.SearchHandlerException;
import com.tigerobo.searchhandler.service.ESService;
import com.tigerobo.searchhandler.service.SearchService;
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

        return esService.simpleSearch(param);
    }
}
