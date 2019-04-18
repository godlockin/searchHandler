package searchhandler.service;

import searchhandler.exception.SearchHandlerException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.util.List;
import java.util.Map;

public interface ESService {

    Integer bulkInsert(String idKey, Map data);
    Integer bulkInsert(String index, String type, String idKey, Map data);

    String getESHttpAddr(Boolean isRandom);
    RestClient getESClient() throws SearchHandlerException;
    RestHighLevelClient getESHighLevelClient() throws SearchHandlerException;

    Map complexSearch(Map param) throws SearchHandlerException;

    Integer bulkInsert(String idKey, List dataList);
    Integer bulkInsert(String index, String type, String idKey, List data);

    Map simpleSearch(Map param) throws SearchHandlerException;

    Map doAnalyze(Map param) throws SearchHandlerException;

    void initESClient() throws SearchHandlerException;
}
