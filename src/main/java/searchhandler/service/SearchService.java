package searchhandler.service;

import searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface SearchService {
    Map doSimpleSearch(Map param) throws SearchHandlerException;

    Map doComplexSearch(Map param) throws SearchHandlerException;

    Map doAnalyze(Map param) throws SearchHandlerException;
}
