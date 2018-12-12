package com.tigerobo.searchhandler.service;

import com.tigerobo.searchhandler.exception.SearchHandlerException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

public interface ESService {

    String getESHttpAddr(Boolean isRandom);
    RestClient getESClient() throws SearchHandlerException;
    RestHighLevelClient getESHighLevelClient() throws SearchHandlerException;
    Integer bulkInsert(String index, String type, String idKey, List data) throws SearchHandlerException;
    Integer bulkInsert(String index, String type, List data) throws SearchHandlerException;
    Map simpleSearch(Map param) throws SearchHandlerException;
}
