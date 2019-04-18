package searchhandler.service;

import searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface ConfigService {
    Map listConfig();

    Map updConfig(Map<String, Object> config) throws SearchHandlerException;
}
