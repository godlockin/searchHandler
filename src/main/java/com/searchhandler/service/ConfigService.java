package com.searchhandler.service;

import com.searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface ConfigService {
    Map listConfig();

    Map updConfig(Map<String, Object> config) throws SearchHandlerException;
}
