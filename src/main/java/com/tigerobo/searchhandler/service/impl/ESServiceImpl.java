package com.tigerobo.searchhandler.service.impl;

import com.tigerobo.searchhandler.common.constants.BusinessConstants;
import com.tigerobo.searchhandler.common.constants.ResultEnum;
import com.tigerobo.searchhandler.common.utils.DataUtils;
import com.tigerobo.searchhandler.exception.SearchHandlerException;
import com.tigerobo.searchhandler.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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
    private int ES_BULK_SIZE;
    @Value("${elasticsearch.bulk.flush}")
    private int ES_BULK_FLUSH;
    @Value("${elasticsearch.bulk.concurrent}")
    private int ES_BULK_CONCURRENT;
    @Value("${elasticsearch.connect-timeout}")
    private int ES_CONNECT_TIMEOUT;
    @Value("${elasticsearch.socket-timeout}")
    private int ES_SOCKET_TIMEOUT;
    @Value("${elasticsearch.connection-request-timeout}")
    private int ES_CONNECTION_REQUEST_TIMEOUT;
    @Value("${elasticsearch.max-retry-tineout-millis}")
    private int ES_MAX_RETRY_TINEOUT_MILLIS;

    @Value("${elasticsearch.index}")
    private String esIndex;
    @Value("${elasticsearch.type}")
    private String esType;

    private RequestOptions COMMON_OPTIONS = RequestOptions.DEFAULT.toBuilder().build();
    private static RestClient restClient;
    private static RestHighLevelClient restHighLevelClient;
    private static BulkProcessor bulkProcessor;
    private List<String> esHttpAddress = new ArrayList<>();
    private AtomicInteger bulkCount = new AtomicInteger(0);

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

    private Map<String, Object> buildResult(SearchResponse response) {
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
        dataList.stream().forEach(x -> {
                    Map data = (Map) x;
                    String pk = DataUtils.getNotNullValue(data, idKey, String.class, "");
                    bulkCommit(new IndexRequest(index, type, pk).source(data));
                });

        int size = dataList.size();
        log.info("Bulk inserted {} data", size);
        return size;
    }

    private void bulkCommit(DocWriteRequest request) {

        /*if (bulkCount.incrementAndGet() >= ES_BULK_SIZE) {
            synchronized (ESServiceImpl.class) {
                if (bulkCount.get() >= ES_BULK_SIZE) {
                    log.info("Reach the bulk gap:[{}] refresh the client", ES_BULK_SIZE);
                    // reset es client
                    // and reset counter
                    closeESClient();
                    initESClient();
                    bulkCount.set(0);
                    log.info("Client refresh done");
                }
            }
        }*/

        bulkProcessor.add(request);
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
    private void initESClient() throws SearchHandlerException {
        initStaticVariables();
        Integer esHttpPort = (null == ES_HTTP_PORT || 0 == ES_HTTP_PORT) ? BusinessConstants.ESConfig.DEFAULT_ES_HTTP_PORT : ES_HTTP_PORT;
        String esAddrs = (StringUtils.isNotBlank(ES_ADDRESSES)) ? ES_ADDRESSES : BusinessConstants.ESConfig.DEFAULT_ES_ADDRESSES;

        try {
            String[] addrArry = esAddrs.split(",");
            int size = addrArry.length;
            HttpHost[] httpHosts = new HttpHost[size];
            for (int i = 0; i < size; i++) {
                String addr = addrArry[i];
                esHttpAddress.add(addr + esHttpPort);
                httpHosts[i] = new HttpHost(addr, esHttpPort, "http");
            }

            RestClientBuilder builder = RestClient.builder(httpHosts)
                    .setRequestConfigCallback((RequestConfig.Builder requestConfigBuilder) ->
                            requestConfigBuilder.setConnectTimeout(ES_CONNECT_TIMEOUT)
                                    .setSocketTimeout(ES_SOCKET_TIMEOUT)
                                    .setConnectionRequestTimeout(ES_CONNECTION_REQUEST_TIMEOUT))
                                    .setMaxRetryTimeoutMillis(ES_MAX_RETRY_TINEOUT_MILLIS);

            restClient = builder.build();
            restHighLevelClient = new RestHighLevelClient(builder);

            bulkProcessor = BulkProcessor.builder((request, bulkListener) ->
                    restHighLevelClient.bulkAsync(request, COMMON_OPTIONS, bulkListener),
                    getBPListener())
                    .setBulkActions(ES_BULK_FLUSH)
                    .setBulkSize(new ByteSizeValue(ES_BULK_SIZE, ByteSizeUnit.MB))
                    .setFlushInterval(TimeValue.timeValueSeconds(10L))
                    .setConcurrentRequests(ES_BULK_CONCURRENT)
                    .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L), 3))
                    .build();
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we init ES transport client" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_INIT);
        }
    }

    private BulkProcessor.Listener getBPListener() {
        return new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("Start to handle bulk commit executionId:[{}] for {} requests", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                log.info("Finished handling bulk commit executionId:[{}]", executionId);

                if (response.hasFailures()) {
                    List<String> errMsg = new ArrayList<>();
                    response.spliterator().forEachRemaining(x -> {
                        if (x.isFailed()) {
                            errMsg.add(String.format("\tid:[%s], item:[%s]: %s", x.getId(), x.getItemId(), x.getFailureMessage()));
                        }
                    });
                    log.error("Bulk executionId:[{}] has error messages:\n", executionId, String.join("\n", errMsg));
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                failure.printStackTrace();
                log.error("Bulk finished with error:[{}]", failure.getMessage());
                request.requests().stream().filter(x -> x instanceof IndexRequest)
                        .forEach(x -> {
                            Map source = ((IndexRequest) x).sourceAsMap();
                            String pk = DataUtils.getNotNullValue(source, "id", String.class, "");
                            log.error("Failure to handle index:[{}], type:[{}] id:[{}]", x.index(), x.type(), pk);
                        });
            }
        };
    }

    private void initStaticVariables() {

        ES_BULK_SIZE = DataUtils.handleNullValue(ES_BULK_SIZE, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_SIZE);
        ES_BULK_FLUSH = DataUtils.handleNullValue(ES_BULK_FLUSH, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_FLUSH);
        ES_BULK_CONCURRENT = DataUtils.handleNullValue(ES_BULK_CONCURRENT, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_CONCURRENT);
        ES_CONNECT_TIMEOUT = DataUtils.handleNullValue(ES_CONNECT_TIMEOUT, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_CONNECT_TIMEOUT);
        ES_SOCKET_TIMEOUT = DataUtils.handleNullValue(ES_SOCKET_TIMEOUT, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_SOCKET_TIMEOUT);
        ES_CONNECTION_REQUEST_TIMEOUT = DataUtils.handleNullValue(ES_CONNECTION_REQUEST_TIMEOUT, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT);
        ES_MAX_RETRY_TINEOUT_MILLIS = DataUtils.handleNullValue(ES_MAX_RETRY_TINEOUT_MILLIS, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_MAX_RETRY_TINEOUT_MILLIS);
    }

    private void closeESClient() throws SearchHandlerException {
        try {
            boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
            if (terminated) {
                restClient.close();
                restHighLevelClient.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to close ES client" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_CLOSE);
        }
    }
}
