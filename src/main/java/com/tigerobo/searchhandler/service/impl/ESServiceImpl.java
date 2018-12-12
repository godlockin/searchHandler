package com.tigerobo.searchhandler.service.impl;

import com.alibaba.fastjson.JSON;
import com.tigerobo.searchhandler.common.constants.BusinessConstants;
import com.tigerobo.searchhandler.common.constants.ResultEnum;
import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.exception.SearchHandlerException;
import com.tigerobo.searchhandler.service.ESService;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class ESServiceImpl implements ESService {

    @Value("${elasticsearch.address}")
    private String ES_ADDRESSES;
    @Value("${elasticsearch.port.http}")
    private Integer ES_HTTP_PORT;
    @Value("${elasticsearch.bulk.size}")
    private Long ES_BULK_SIZE;

    @Value("${elasticsearch.index}")
    private String esIndex;
    @Value("${elasticsearch.type}")
    private String esType;

    private RequestOptions COMMON_OPTIONS = RequestOptions.DEFAULT.toBuilder().build();
    private static RestClient restClient;
    private static RestHighLevelClient restHighLevelClient;
    private List<String> esHttpAddress = new ArrayList<>();
    private AtomicLong bulkCount = new AtomicLong(0L);

    @Override
    public Map simpleSearch(Map param) throws SearchHandlerException {

        Map result = new HashMap();
        String trgtIndex = DataUtils.getNotNullValue(param, "index", String.class, esIndex);
        String trgtType = DataUtils.getNotNullValue(param, "type", String.class, esType);
        if (StringUtils.isBlank(trgtIndex) || StringUtils.isBlank(trgtType)) {
            log.error("Can't find index:[{}] or type:[{}] info", trgtIndex, trgtType);
            return result;
        }

        SearchRequest searchRequest = new SearchRequest().indices(trgtIndex).types(trgtType);
        try {

            Integer from = DataUtils.getNotNullValue(param, "from", Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_FROM);
            Integer size = DataUtils.getNotNullValue(param, "size", Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_SIZE);
            if (from + size > BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE) {
                result.put("Message", "Over size limit, please try scroll");
                log.error("Over size limit, please try scroll");
                size = BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE - from;
            }

            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().from(from).size(size);
            Object queryItem = DataUtils.getNotNullValue(param, "text", Object.class, "");
            List<String> fieldList = DataUtils.getNotNullValue(param, "fields", List.class, new ArrayList<>());
            String[] fieldArr = fieldList.toArray(new String[fieldList.size()]);
            MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(queryItem, fieldArr);
            sourceBuilder.query(multiMatchQueryBuilder);

            HighlightBuilder highlightBuilder = new HighlightBuilder();
            List<String> highlightList = DataUtils.getNotNullValue(param, "highlight", List.class, new ArrayList<>());
            highlightList.stream().forEach(x -> highlightBuilder.field(x));
            sourceBuilder.highlighter(highlightBuilder);

            searchRequest.source(sourceBuilder);
            log.info("Try to query as request:[{}]", searchRequest);
            SearchResponse response = restHighLevelClient.search(searchRequest, COMMON_OPTIONS);
            log.info("Got response as:[{}]", response);
            result.putAll(buildResult(response));
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to query ES as:" + searchRequest;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_QUERY);
        }
    }

    private Map buildResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        Map result = new HashMap(){{
           put("total", hits.totalHits);
        }};

        List dataList = Stream.of(hits.getHits()).map(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            Map<String, HighlightField> highlightFields = x.getHighlightFields();
            if (!highlightFields.isEmpty()) {
                Map highlight = new HashMap();
                highlightFields.forEach((k, v) -> highlight.put(k, v.fragments()[0].string()));
                sourceAsMap.put("hightlight", highlight);
            }
            return sourceAsMap;
        }).collect(Collectors.toList());

        result.put("data", dataList);
        return result;
    }

    @Override
    public Integer bulkInsert(String index, String type, String idKey, List dataList) throws SearchHandlerException {

        if (StringUtils.isBlank(index) || StringUtils.isBlank(type) || CollectionUtils.isEmpty(dataList)) {
            log.error("Important info missing for index:[{}] type:[{}] and data:[{}]", index, type, dataList);
            return 0;
        }

        log.info("Try to bulk insert into index:[{}] type:[{}]", index, type);
        BulkRequest bulkRequest = new BulkRequest();
        dataList.stream().forEach(x -> {
            Map data = (Map) x;
            IndexRequest indexRequest = new IndexRequest(index, type);
            if (StringUtils.isNotBlank(idKey)) {
                String pk = DataUtils.getNotNullValue(data, idKey, String.class, "");
                if (StringUtils.isNotBlank(pk)) {
                    indexRequest = new IndexRequest(index, type, pk);
                }
            }
            indexRequest.source(data);
            bulkRequest.add(indexRequest);
        });

        Integer dataSize = dataList.size();
        bulkCommit(dataSize, bulkRequest);
        log.info("Bulk inserted {} data", dataSize);
        return dataSize;

    }

    @Override
    public Integer bulkInsert(String index, String type, List data) throws SearchHandlerException {

        return bulkInsert(index, type, "", data);
    }

    private Integer bulkCommit(Integer bulkSize, BulkRequest bulkRequest) throws SearchHandlerException {

        if (bulkCount.get() >= ES_BULK_SIZE) {
            synchronized (ESServiceImpl.class) {
                if (bulkCount.get() >= ES_BULK_SIZE) {
                    log.info("Reach the bulk gap:[{}] refresh the client", ES_BULK_SIZE);
                    // reset es client
                    // and reset counter
                    closeESClient();
                    initESClient();
                    bulkCount.set(0L);
                    log.info("Client refresh done");
                }
            }
        }

        try {
            bulkCount.addAndGet(bulkSize);
            restHighLevelClient.bulk(bulkRequest, COMMON_OPTIONS);
            return bulkSize;
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we do bulk insert" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_BULK_COMMIT);
        }
    }

    @Override
    public String getESHttpAddr(Boolean isRandom) {

        int index = (isRandom) ? new Random().nextInt(esHttpAddress.size()) : 0;
        return esHttpAddress.get(index);
    }

    @Override
    public RestClient getESClient() throws SearchHandlerException {
        if (null == restClient) {
            synchronized (ESServiceImpl.class) {
                if (null == restClient) {
                    initESClient();
                }
            }
        }
        return restClient;
    }

    @Override
    public RestHighLevelClient getESHighLevelClient() throws SearchHandlerException {

        if (null == restHighLevelClient) {
            synchronized (ESServiceImpl.class) {
                if (null == restHighLevelClient) {
                    initESClient();
                }
            }
        }
        return restHighLevelClient;
    }

    @PostConstruct
    void initESClient() throws SearchHandlerException {
        Integer esHttpPort = (null == ES_HTTP_PORT || 0 == ES_HTTP_PORT) ? BusinessConstants.ESConfig.DEFAULT_ES_HTTP_PORT : ES_HTTP_PORT;
        String esAddrs = (StringUtils.isNotBlank(ES_ADDRESSES)) ? ES_ADDRESSES : BusinessConstants.ESConfig.DEFAULT_ES_ADDRESSES;
        ES_BULK_SIZE = (null == ES_BULK_SIZE || 0L == ES_BULK_SIZE) ? BusinessConstants.ESConfig.DEFAULT_ES_BULK_SIZE : ES_BULK_SIZE;

        try {
            String[] addrArry = esAddrs.split(",");
            int size = addrArry.length;
            HttpHost[] httpHosts = new HttpHost[size];
            for (int i = 0; i < size; i++) {
                String addr = addrArry[i];
                esHttpAddress.add(addr + esHttpPort);
                httpHosts[i] = new HttpHost(addr, esHttpPort, "http");
            }
            RestClientBuilder builder = RestClient.builder(httpHosts);
            restClient = builder.build();
            restHighLevelClient = new RestHighLevelClient(builder);
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we init ES transport client" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_INIT);
        }
    }

    void closeESClient() throws SearchHandlerException {
        try {
            restClient.close();
            restHighLevelClient.close();
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to close ES client" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_CLOSE);
        }
    }
}
