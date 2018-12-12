package com.tigerobo.searchhandler.service;

import com.tigerobo.searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface SearchService {
    Map doSimpleSearch(Map param) throws SearchHandlerException;
}
