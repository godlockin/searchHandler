package searchhandler.service.impl;

import searchhandler.common.LocalConfig;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.service.ESService;
import searchhandler.common.constants.ResultEnum;
import searchhandler.common.utils.DataUtils;
import searchhandler.exception.SearchHandlerException;
import lombok.Data;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.*;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.ParsedSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.terms.IncludeExclude;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.stats.Stats;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.*;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Data
@Slf4j
@Service
@SuppressWarnings({ "unchecked" })
public class ESServiceImpl implements ESService {

    private String ES_TYPE;
    private String ES_INDEX;
    private String ES_ADDRESSES;
    private Integer ES_HTTP_PORT;

    private int ES_BULK_SIZE;
    private int ES_BULK_FLUSH;
    private int ES_SOCKET_TIMEOUT;
    private int ES_CONNECT_TIMEOUT;
    private int ES_BULK_CONCURRENT;
    private int ES_MAX_RETRY_TINEOUT_MILLIS;
    private int ES_CONNECTION_REQUEST_TIMEOUT;

    private RequestOptions COMMON_OPTIONS = RequestOptions.DEFAULT.toBuilder().build();
    private static RestClient restClient;
    private static RestHighLevelClient restHighLevelClient;
    private static BulkProcessor bulkProcessor;
    private List<String> esHttpAddress = new ArrayList<>();
    private AtomicInteger bulkCount = new AtomicInteger(0);

    @Override
    public Map simpleSearch(Map param) throws SearchHandlerException {

        SearchSourceBuilder sourceBuilder = simpleSearchBuilder(param);

        return fullSearch(param, sourceBuilder);
    }

    @Override
    public Map doAnalyze(Map param) {

        Map result = getBaseResult();

        List textList = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.QUERY_KEY, List.class, new ArrayList<>());
        if (textList.isEmpty()) {
            log.error("No trgt text found");
            return result;
        }

        AnalyzeRequest request = new AnalyzeRequest();

        // text(s)
        textList.parallelStream().forEach(x -> request.text(String.valueOf(x).trim()));

        // analyzer
        request.analyzer(DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.ANALYZER_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_ANALYZER));

        // set index if exists
        request.index((String) param.get(BusinessConstants.ESConfig.INDEX_KEY));

        // set field if exists
        request.field((String) param.get(BusinessConstants.ESConfig.FIELD_KEY));

        // char filters
        List charFilters = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.CHAR_FILTER_KEY, List.class, new ArrayList<>());
        charFilters.parallelStream().forEach(x -> {
            if (x instanceof String)
                request.addCharFilter((String) x);
            if (x instanceof Map)
                request.addCharFilter((Map) x);
        });

        // token filter
        List tokenFilters = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.TOKEN_FILTER_KEY, List.class, new ArrayList<>());
        tokenFilters.parallelStream().forEach(x -> {
            if (x instanceof String)
                request.addTokenFilter((String) x);
            if (x instanceof Map)
                request.addTokenFilter((Map) x);
        });

        // tokenizer
        Object tokenizer = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.TOKENIZER_KEY, Object.class, new Object());
        if (tokenizer instanceof String)
            request.tokenizer((String) tokenizer);
        if (tokenizer instanceof Map)
            request.tokenizer((Map) tokenizer);

        // normalizer
        request.normalizer((String) param.get(BusinessConstants.ESConfig.NORMALIZER_KEY));

        try {
            AnalyzeResponse response = restHighLevelClient.indices().analyze(request, COMMON_OPTIONS);
            List terms = Optional.ofNullable(response.getTokens()).orElse(new ArrayList<>())
                    .stream().map(AnalyzeResponse.AnalyzeToken::getTerm).collect(Collectors.toList());
            result.put(BusinessConstants.ResultConfig.DATA_KEY, terms);
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we do analyze for:[{}], {}", textList, e);
        }

        return result;
    }

    @Override
    public Map complexSearch(Map param) throws SearchHandlerException {
        SearchSourceBuilder sourceBuilder = makeBaseSearchBuilder(param);

        boolean isQuery = false;
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        // build query conditions
        Map query = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.QUERY_KEY, Map.class, new HashMap<>());
        if (!query.isEmpty()) {
            boolQueryBuilder.must(buildBoolQuery(query));
            isQuery = true;
        }

        // build filter conditions
        Map filter = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.FILTER_KEY, Map.class, new HashMap<>());
        if (!filter.isEmpty()) {
            boolQueryBuilder.filter(buildBoolQuery(filter));
            isQuery = true;
        }

        // set query & filter conditions
        if (isQuery) {
            sourceBuilder.query(boolQueryBuilder);
        }

        // build aggregation
        Map aggregationInfo = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.AGGREGATION_KEY, Map.class, new HashMap<>());
        if (!aggregationInfo.isEmpty()) {
            aggregationInfo.forEach((k, v) -> sourceBuilder.aggregation(buildCommonAgg((Map) v)));
        }

        // if highlight exists
        List<String> highlightList = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.HIGHLIGHT_KEY, List.class, new ArrayList<>());
        if (!highlightList.isEmpty()) {
            HighlightBuilder highlightBuilder = new HighlightBuilder();
            highlightList.parallelStream().forEach(highlightBuilder::field);
            sourceBuilder.highlighter(highlightBuilder);
        }

        Map fetchSrouce = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.FETCHSOURCE_KEY, Map.class, new HashMap<>());
        if (!fetchSrouce.isEmpty()) {
            List includeList = DataUtils.getNotNullValue(fetchSrouce, BusinessConstants.ESConfig.INCLUDE_KEY, List.class, new ArrayList<>());
            String[] includeFields = (includeList.isEmpty()) ? new String[0] : (String[]) includeList.parallelStream().toArray(String[]::new);
            List excludeList = DataUtils.getNotNullValue(fetchSrouce, BusinessConstants.ESConfig.EXCLUDE_KEY, List.class, new ArrayList<>());
            String[] excludeFields = (excludeList.isEmpty()) ? new String[0] : (String[]) excludeList.parallelStream().toArray(String[]::new);
            sourceBuilder.fetchSource(includeFields, excludeFields);
        }

        List sortConfig = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SORT_KEY, List.class, new ArrayList<>());
        if (!sortConfig.isEmpty()) {
            doBuildSorts(sourceBuilder, sortConfig);
        }

        return fullSearch(param, sourceBuilder);
    }

    private void doBuildSorts(SearchSourceBuilder sourceBuilder, List sortConfig) {

        sortConfig.parallelStream().forEach(x -> {
            Map config = (Map) x;
            String type = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, String.class, "");
            String order = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.ORDER_KEY, String.class, "");
            if (BusinessConstants.ESConfig.FIELD_SORT_TYPE.equalsIgnoreCase(type)) {
                String field = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SIMPLE_QUERY_FIELD_KEY, String.class, "");
                FieldSortBuilder fieldSortBuilder = SortBuilders.fieldSort(field);
                fieldSortBuilder.order(SortOrder.fromString(order));
                sourceBuilder.sort(fieldSortBuilder);
            } else if (BusinessConstants.ESConfig.SCRIPT_SORT_TYPE.equalsIgnoreCase(type)) {
                String scriptStr = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_SORT_TYPE, String.class, "");
                String scriptSortTypeStr = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_SORT_SCRIPT_TYPE, String.class, BusinessConstants.ESConfig.NUMBER_TYPE);
                String sortMode = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SORT_MODE, String.class, "");
                String scriptTypeStr = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_TYPE, String.class, BusinessConstants.ESConfig.INLINE_TYPE);
                String scriptLangStr = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_LANG, String.class, BusinessConstants.ESConfig.PAINLESS_TYPE);
                Map options = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_OPTIONS, Map.class, Collections.emptyMap());
                Map params = DataUtils.getNotNullValue(config, BusinessConstants.ESConfig.SCRIPT_PARAMS, Map.class, Collections.emptyMap());
                if (StringUtils.isNotBlank(scriptStr)) {
                    ScriptType scriptType = (BusinessConstants.ESConfig.INLINE_TYPE.equalsIgnoreCase(scriptTypeStr)) ? ScriptType.INLINE : ScriptType.STORED;
                    Script script = new Script(scriptType, scriptLangStr, scriptStr, options, params);
                    ScriptSortBuilder.ScriptSortType scriptSortType = ScriptSortBuilder.ScriptSortType.fromString(scriptSortTypeStr);
                    ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script, scriptSortType);
                    scriptSortBuilder.order(SortOrder.fromString(order));
                    if (StringUtils.isNotBlank(sortMode)) {
                        scriptSortBuilder.sortMode(SortMode.fromString(sortMode));
                    }
                    sourceBuilder.sort(scriptSortBuilder);
                }
            }
        });
    }

    private AggregationBuilder buildCommonAgg(Map param) {
        String type = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, String.class, "");
        String aggrName = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_NAME_KEY, String.class, "");
        String field = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_FIELD_KEY, String.class, "");
        if (BusinessConstants.ESConfig.SIMPLE_AGGREGATION_LIST.contains(type)) {
            switch (type) {
                case BusinessConstants.ESConfig.COUNT_KEY:
                    return AggregationBuilders.count(aggrName).field(field);
                case BusinessConstants.ESConfig.SUM_KEY:
                    return AggregationBuilders.sum(aggrName).field(field);
                case BusinessConstants.ESConfig.MAX_KEY:
                    return AggregationBuilders.max(aggrName).field(field);
                case BusinessConstants.ESConfig.MIN_KEY:
                    return AggregationBuilders.min(aggrName).field(field);
                case BusinessConstants.ESConfig.AVG_KEY:
                    return AggregationBuilders.avg(aggrName).field(field);
                case BusinessConstants.ESConfig.STATS_KEY:
                    return AggregationBuilders.stats(aggrName).field(field);
                case BusinessConstants.ESConfig.TERMS_KEY:
                    return getTermsAggregation(param, aggrName, field);
            }
        } else if (BusinessConstants.ESConfig.NESTED_KEY.equalsIgnoreCase(type)) {
            List subAgg = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SUB_AGG_KEY, List.class, new ArrayList<>());
            String path = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.PATH_KEY, String.class, "");
            AggregationBuilder aggregationBuilder = AggregationBuilders.nested(aggrName, path);
            subAgg.parallelStream().forEach(x -> aggregationBuilder.subAggregation(buildCommonAgg((Map) x)));
            return aggregationBuilder;
        }

        String from = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.FROM_KEY, String.class, "");
        String to = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.TO_KEY, String.class, "");
        return AggregationBuilders.dateRange(field).addRange(from, to);
    }

    private AggregationBuilder getTermsAggregation(Map param, String aggrName, String field) {
        TermsAggregationBuilder termsAgg = AggregationBuilders.terms(aggrName).field(field);
        Integer size = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_SIZE_KEY, Integer.class, 0);
        if (0 < size) {
            termsAgg.size(size);
        }

        Integer shard_size = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SHARD_SIZE_KEY, Integer.class, 0);
        if (0 < shard_size) {
            termsAgg.shardSize(shard_size);
        }

        Long minDocCount = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < minDocCount) {
            termsAgg.minDocCount(minDocCount);
        }

        Long shardMinDocCount = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SHARD_MIN_DOC_COUNT_KEY, Integer.class, 0).longValue();
        if (0L < shardMinDocCount) {
            termsAgg.shardMinDocCount(shardMinDocCount);
        }

        termsAgg.missing(DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.MISSING_KEY, String.class, ""));

        Object includeObj = param.get(BusinessConstants.ESConfig.INCLUDE_KEY);
        Object excludeObj = param.get(BusinessConstants.ESConfig.EXCLUDE_KEY);
        if (null != includeObj || null != excludeObj) {
            IncludeExclude includeExclude = null;
            if (includeObj instanceof String || excludeObj instanceof String) {
                includeExclude = new IncludeExclude(Optional.ofNullable(includeObj).orElse("").toString()
                        , Optional.ofNullable(includeObj).orElse("").toString());
            } else if (includeObj instanceof List || excludeObj instanceof List) {
                List includeList = (List) Optional.ofNullable(includeObj).orElse(new ArrayList<>());
                List excludeList = (List) Optional.ofNullable(excludeObj).orElse(new ArrayList<>());
                Object firstItem = null;
                if (!includeList.isEmpty()) {
                    firstItem = includeList.get(0);
                } else if (!excludeList.isEmpty()) {
                    firstItem = excludeList.get(0);
                }

                if (null != firstItem) {
                    if (firstItem instanceof String) {
                        String[] inArr = (String[]) includeList.parallelStream().toArray(String[]::new);
                        String[] exArr = (String[]) excludeList.parallelStream().toArray(String[]::new);
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Double) {
                        double[] inArr = new double[includeList.size()];
                        double[] exArr = new double[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (double) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (double) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    } else if (firstItem instanceof Long) {
                        long[] inArr = new long[includeList.size()];
                        long[] exArr = new long[excludeList.size()];
                        int inIndex = 0;
                        DataUtils.forEach(inIndex, includeList, (index, list) -> inArr[index] = (long) ((List) list).get(index));
                        int exIndex = 0;
                        DataUtils.forEach(exIndex, excludeList, (index, list) -> exArr[index] = (long) ((List) list).get(index));
                        includeExclude = new IncludeExclude((includeList.isEmpty()) ? null : inArr, (excludeList.isEmpty()) ? null : exArr);
                    }
                }
            }

            if (null != includeExclude) {
                termsAgg.includeExclude(includeExclude);
            }
        }
        return termsAgg;
    }

    private BoolQueryBuilder buildBoolQuery(Map param) {

        BoolQueryBuilder query = QueryBuilders.boolQuery();
        param.keySet().parallelStream().filter(BusinessConstants.ESConfig.BOOL_CONDITION_LIST::contains).forEach(x -> {
            String key = (String) x;
            List trgt = DataUtils.getNotNullValue(param, key, List.class, new ArrayList<>());
            switch (key) {
                case BusinessConstants.ESConfig.MUST_KEY:
                    trgt.parallelStream().forEach(y -> query.must(buildCommonQuery().apply((Map) y)));
                    break;
                case BusinessConstants.ESConfig.SHOULD_KEY:
                    trgt.parallelStream().forEach(y -> query.should(buildCommonQuery().apply((Map) y)));
                    break;
                case BusinessConstants.ESConfig.MUST_NOT_KEY:
                    trgt.parallelStream().forEach(y -> query.mustNot(buildCommonQuery().apply((Map) y)));
                    break;
            }
        });
        return query;
    }

    private Function<Map, QueryBuilder> buildCommonQuery() {

        return (param) -> {
            String type = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, String.class, "");
            if (BusinessConstants.ESConfig.SIMPLE_CONDITION_LIST.contains(type)) {
                return buildSimpleQuery(param);
            } else if (BusinessConstants.ESConfig.RANGE_KEY.equalsIgnoreCase(type)) {
                return buildRangeQuery(param);
            } else if (BusinessConstants.ESConfig.MULTIMATCH_KEY.equalsIgnoreCase(type)) {
                return buildMultiMatchQuery(param);
            } else if (BusinessConstants.ESConfig.NESTED_KEY.equalsIgnoreCase(type)) {
                return buildNestedQuery(param);
            }
            return QueryBuilders.matchAllQuery();
        };
    }

    private QueryBuilder buildNestedQuery(Map param) {
        String path = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.PATH_KEY, String.class, "");
        Map query = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.QUERY_KEY, Map.class, new HashMap<>());
        return QueryBuilders.nestedQuery(path, buildCommonQuery().apply(query), ScoreMode.Avg);
    }

    private QueryBuilder buildMultiMatchQuery(Map param) {
        Object value = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_VALUE_KEY, Object.class, new Object());
        Object fieldNames = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.FIELDNAMES_KEY, Object.class, new Object());
        Collection fieldNamesCollection = (fieldNames instanceof Collection) ? (Collection) fieldNames : Collections.singletonList(fieldNames);
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(value, new String[0]);
        fieldNamesCollection.parallelStream().forEach(x -> {
            String nameStr = (String) x;
            if (0 > nameStr.indexOf('^')) {
                multiMatchQueryBuilder.field(nameStr);
            } else {
                String[] arr = nameStr.split("\\^");
                multiMatchQueryBuilder.field(arr[0], Float.valueOf(arr[1]));
            }
        });
        return multiMatchQueryBuilder;
    }

    private QueryBuilder buildRangeQuery(Map param) {
        Object field = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_FIELD_KEY, Object.class, new Object());
        RangeQueryBuilder queryBuilder = QueryBuilders.rangeQuery(String.valueOf(field).trim());
        param.keySet().parallelStream().forEach(x -> {
            String key = (String) x;
            Object value = DataUtils.getNotNullValue(param, key, Object.class, new Object());
            switch (key) {
                case BusinessConstants.ESConfig.INCLUDE_LOWER_KEY:
                    queryBuilder.includeLower((Boolean) value);
                    break;
                case BusinessConstants.ESConfig.INCLUDE_UPPER_KEY:
                    queryBuilder.includeUpper((Boolean) value);
                    break;
                case BusinessConstants.ESConfig.FROM_KEY:
                    queryBuilder.from(value);
                    break;
                case BusinessConstants.ESConfig.LTE_KEY:
                    queryBuilder.lte(value);
                    break;
                case BusinessConstants.ESConfig.GTE_KEY:
                    queryBuilder.gte(value);
                    break;
                case BusinessConstants.ESConfig.LT_KEY:
                    queryBuilder.lt(value);
                    break;
                case BusinessConstants.ESConfig.GT_KEY:
                    queryBuilder.gt(value);
                    break;
                case BusinessConstants.ESConfig.TO_KEY:
                    queryBuilder.to(value);
                    break;
                default:
                    break;
            }
        });
        return queryBuilder;
    }

    private QueryBuilder buildSimpleQuery(Map param) {

        String type = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_TYPE_KEY, String.class, "");
        Object field = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_FIELD_KEY, Object.class, new Object());
        Object value = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SIMPLE_QUERY_VALUE_KEY, Object.class, new Object());

        switch (type) {
            case BusinessConstants.ESConfig.MATCH_KEY:
                return QueryBuilders.matchQuery(String.valueOf(field).trim(), value);
            case BusinessConstants.ESConfig.TERM_KEY:
                return QueryBuilders.termQuery(String.valueOf(field).trim(), value);
            case BusinessConstants.ESConfig.FUZZY_KEY:
                return QueryBuilders.fuzzyQuery(String.valueOf(field).trim(), value);
            case BusinessConstants.ESConfig.PREFIX_KEY:
                return QueryBuilders.prefixQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case BusinessConstants.ESConfig.REGEXP_KEY:
                return QueryBuilders.regexpQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case BusinessConstants.ESConfig.WRAPPER_KEY:
                return QueryBuilders.wrapperQuery(String.valueOf(value).trim());
            case BusinessConstants.ESConfig.WILDCARD_KEY:
                return QueryBuilders.wildcardQuery(String.valueOf(field).trim(), String.valueOf(value).trim());
            case BusinessConstants.ESConfig.COMMONTERMS_KEY:
                return QueryBuilders.commonTermsQuery(String.valueOf(field).trim(), value);
            case BusinessConstants.ESConfig.QUERY_STRING_KEY:
                return QueryBuilders.queryStringQuery(String.valueOf(value).trim());
            case BusinessConstants.ESConfig.MATCH_PHRASE_KEY:
                return QueryBuilders.matchPhraseQuery(String.valueOf(field).trim(), value);
            case BusinessConstants.ESConfig.MATCH_PHRASE_PREFIX_KEY:
                return QueryBuilders.matchPhrasePrefixQuery(String.valueOf(field).trim(), value);
            default:
                return QueryBuilders.termsQuery(String.valueOf(field).trim(), (Collection<?>) value);
        }
    }

    private SearchSourceBuilder simpleSearchBuilder(Map param) {
        SearchSourceBuilder sourceBuilder = makeBaseSearchBuilder(param);
        Object queryItem = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.QUERY_KEY, Object.class, "");
        List<String> fieldList = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.FIELDNAMES_KEY, List.class, new ArrayList<>());
        String[] fieldArr = fieldList.parallelStream().toArray(String[]::new);
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(queryItem, fieldArr);
        sourceBuilder.query(multiMatchQueryBuilder);

        HighlightBuilder highlightBuilder = new HighlightBuilder();
        DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.HIGHLIGHT_KEY, List.class, new ArrayList<>()).parallelStream()
                .forEach(x -> highlightBuilder.field((String) x));
        sourceBuilder.highlighter(highlightBuilder);
        return sourceBuilder;
    }

    private SearchSourceBuilder makeBaseSearchBuilder(Map param) {
        Integer from = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_FROM, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_FROM);
        Integer size = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_SIZE, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_SIZE);
        if (from + size > BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE) {
            log.error("Over size limit, please try scroll");
            size = BusinessConstants.ESConfig.DEFAULT_ES_MAX_SIZE - from;
        }

        return new SearchSourceBuilder().from(from).size(size);
    }

    private Map fullSearch(Map param, SearchSourceBuilder sourceBuilder) throws SearchHandlerException {
        String trgtIndex = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_INDEX, String.class, ES_INDEX);
        String trgtType = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_QUERY_TYPE, String.class, ES_TYPE);
        if (StringUtils.isBlank(trgtIndex) || StringUtils.isBlank(trgtType)) {
            log.error("Can't find index:[{}] or type:[{}] info", trgtIndex, trgtType);
            return new HashMap();
        }

        SearchRequest searchRequest = new SearchRequest().indices(trgtIndex).types(trgtType).source(sourceBuilder);

        try {
            Long startTime = System.currentTimeMillis();
            SearchResponse response = doQuery(param, searchRequest);
            log.debug("Got response as:[{}]", response);
            Long endTime = System.currentTimeMillis();
            log.debug("Finish query at:[{}] which took:[{}]", endTime, (endTime - startTime) / 1000);
            return buildResult(response);
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to query ES as:" + searchRequest;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_QUERY);
        }
    }

    private SearchResponse doQuery(Map param, SearchRequest searchRequest) throws IOException  {
        String scrollId = DataUtils.getNotNullValue(param, BusinessConstants.ResultConfig.SCROLL_ID_KEY, String.class, "");
        String timeValue = DataUtils.getNotNullValue(param, BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY, String.class, "");
        if (StringUtils.isNotBlank(timeValue)) {
            TimeValue tv = TimeValue.parseTimeValue(timeValue, BusinessConstants.ESConfig.SCROLL_TIME_VALUE_KEY);
            if (StringUtils.isNotBlank(scrollId)) {
                return restHighLevelClient.scroll(new SearchScrollRequest(scrollId).scroll(tv), COMMON_OPTIONS);
            } else {
                searchRequest.scroll(tv);
            }
        }

        log.info("Try to query at:[{}] with request:[{}]", System.currentTimeMillis(), searchRequest.source().toString());
        return restHighLevelClient.search(searchRequest, COMMON_OPTIONS);
    }

    private Map<String, Object> buildResult(SearchResponse response) {
        SearchHits hits = response.getHits();
        long total = hits.totalHits;
        log.info("Got {} data in total", total);
        Map result = getBaseResult();
        result.put(BusinessConstants.ResultConfig.TOTAL_KEY, hits.totalHits);

        List dataList = Stream.of(hits.getHits()).map(x -> {
            Map<String, Object> sourceAsMap = x.getSourceAsMap();
            sourceAsMap.put(BusinessConstants.ESConfig.SCORE_KEY, x.getScore());
            Map<String, HighlightField> highlightFields = x.getHighlightFields();
            if (!highlightFields.isEmpty()) {
                Map highlight = new HashMap();
                highlightFields.forEach((k, v) -> highlight.put(k, v.fragments()[0].string()));
                sourceAsMap.put(BusinessConstants.ResultConfig.HIGHLIGH_KEY, highlight);
            }
            return sourceAsMap;
        }).collect(Collectors.toList());

        result.put(BusinessConstants.ResultConfig.DATA_KEY, dataList);
        log.debug("Build as {} data", dataList.size());

        Map<String, Object> aggMap = new HashMap<>();
        List<Aggregation> aggregations = Optional.ofNullable(response.getAggregations()).orElse(new Aggregations(Collections.emptyList())).asList();
        aggregations.parallelStream().forEach(aggregation -> getAggrInfo(aggMap, "", aggregation));
        log.debug("Build as {} aggregation data", aggMap.size());
        result.put(BusinessConstants.ResultConfig.AGGREGATION_KEY, aggMap);

        String scrollId = response.getScrollId();
        if (StringUtils.isNotBlank(scrollId)) {
            log.debug("Generated scroll id:[{}]", scrollId);
            result.put(BusinessConstants.ResultConfig.SCROLL_ID_KEY, scrollId);
        }

        return result;
    }

    private void getAggrInfo(Map<String, Object> aggMap, String parentName, Aggregation aggregation) {

        String key = (StringUtils.isNotBlank(parentName)) ? parentName + "." + aggregation.getName() : aggregation.getName();
        if (aggregation instanceof ParsedSingleBucketAggregation) {
            ParsedSingleBucketAggregation parsedNested = (ParsedSingleBucketAggregation) aggregation;
            aggMap.put(key + ".count", Long.toString(parsedNested.getDocCount()));
            List<Aggregation> aggregations = Optional.ofNullable(parsedNested.getAggregations()).orElse(new Aggregations(Collections.emptyList())).asList();
            aggregations.forEach(subAggregation -> getAggrInfo(aggMap, key, subAggregation));
        } else if (aggregation instanceof NumericMetricsAggregation.SingleValue) {
            NumericMetricsAggregation.SingleValue singleValue = (NumericMetricsAggregation.SingleValue) aggregation;
            aggMap.put(key + ".value", singleValue.getValueAsString());
        } else if (aggregation instanceof Stats) {
            Stats stats = (Stats) aggregation;
            Map data = new HashMap() {{
                put(BusinessConstants.ESConfig.MAX_KEY, stats.getMax());
                put(BusinessConstants.ESConfig.MIN_KEY, stats.getMin());
                put(BusinessConstants.ESConfig.AVG_KEY, stats.getAvg());
                put(BusinessConstants.ESConfig.COUNT_KEY, stats.getCount());
                put(BusinessConstants.ESConfig.SUM_KEY, stats.getSum());
            }};
            aggMap.put(key + ".value", data);
        } else if (aggregation instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
            List bucketList = multiBucketsAggregation.getBuckets().parallelStream().map(x ->
                    new HashMap() {{
                        put("key", x.getKey());
                        put("count", x.getDocCount());
                    }}
            ).collect(Collectors.toList());
            aggMap.put(key + ".value", bucketList);
        }
    }

    @Override
    public Integer bulkInsert(String idKey, List dataList) {
        return bulkInsert(ES_INDEX, ES_TYPE, idKey, dataList);
    }

    @Override
    public Integer bulkInsert(String index, String type, String idKey, List dataList) {

        log.debug("Try to bulk insert into index:[{}] type:[{}]", index, type);
        int size = dataList.parallelStream().mapToInt(x -> bulkInsert(index, type, idKey, (Map) x)).sum();
        log.debug("Bulk inserted {} data", size);
        return size;
    }

    @Override
    public Integer bulkInsert(String idKey, Map data) {
        return bulkInsert(ES_INDEX, ES_TYPE, idKey, data);
    }

    @Override
    public Integer bulkInsert(String index, String type, String idKey, Map data) {
        String trgtIndex = (StringUtils.isBlank(index)) ? ES_INDEX : index;
        String trgtType = (StringUtils.isBlank(type)) ? ES_TYPE : type;
        if (StringUtils.isBlank(trgtIndex) || StringUtils.isBlank(trgtType) || CollectionUtils.isEmpty(data)) {
            log.error("Important info missing for index:[{}] type:[{}] and data:[{}]", trgtIndex, trgtType, data);
            return 0;
        }
        String pk = DataUtils.getNotNullValue(data, idKey, String.class, "");
        IndexRequest indexRequest = StringUtils.isNotBlank(pk) ? new IndexRequest(trgtIndex, trgtType, pk) : new IndexRequest(trgtIndex, trgtType);
        bulkCommit(indexRequest.source(data));
        return 1;
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

    private Map getBaseResult() {
        return new HashMap() {{
            put(BusinessConstants.ResultConfig.TOTAL_KEY, 0);
            put(BusinessConstants.ResultConfig.DATA_KEY, new ArrayList<>());
            put(BusinessConstants.ResultConfig.AGGREGATION_KEY, new HashMap<>());
        }};
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

    @Override
    @Synchronized
    @PostConstruct
    public void initESClient() throws SearchHandlerException {
        log.info("Init ES client");
        closeESClient();
        initStaticVariables();

        try {
            HttpHost[] httpHosts = Arrays.stream(ES_ADDRESSES.split(",")).parallel().map(x -> {
                esHttpAddress.add(x + ES_HTTP_PORT);
                return new HttpHost(x, ES_HTTP_PORT, "http");
            }).collect(Collectors.toList()).parallelStream().toArray(HttpHost[]::new);

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
                request.requests().parallelStream().filter(x -> x instanceof IndexRequest)
                        .forEach(x -> {
                            Map source = ((IndexRequest) x).sourceAsMap();
                            String pk = DataUtils.getNotNullValue(source, "id", String.class, "");
                            log.error("Failure to handle index:[{}], type:[{}] id:[{}]", x.index(), x.type(), pk);
                        });
            }
        };
    }

    private void initStaticVariables() {
        esHttpAddress = new ArrayList<>();
        ES_TYPE = LocalConfig.get(BusinessConstants.SysConfig.ES_TYPE_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE);
        ES_INDEX = LocalConfig.get(BusinessConstants.SysConfig.ES_INDEX_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_ES_INDEX);
        ES_HTTP_PORT = LocalConfig.get(BusinessConstants.SysConfig.ES_HTTP_PORT_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_HTTP_PORT);
        ES_ADDRESSES = LocalConfig.get(BusinessConstants.SysConfig.ES_ADDRESSES_KEY, String.class, BusinessConstants.ESConfig.DEFAULT_ES_ADDRESSES);
        ES_BULK_SIZE = LocalConfig.get(BusinessConstants.SysConfig.ES_BULK_SIZE_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_SIZE);
        ES_BULK_FLUSH = LocalConfig.get(BusinessConstants.SysConfig.ES_BULK_FLUSH_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_FLUSH);
        ES_SOCKET_TIMEOUT = LocalConfig.get(BusinessConstants.SysConfig.ES_SOCKET_TIMEOUT_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_SOCKET_TIMEOUT);
        ES_BULK_CONCURRENT = LocalConfig.get(BusinessConstants.SysConfig.ES_BULK_CONCURRENT_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_BULK_CONCURRENT);
        ES_CONNECT_TIMEOUT = LocalConfig.get(BusinessConstants.SysConfig.ES_CONNECT_TIMEOUT_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_CONNECT_TIMEOUT);
        ES_MAX_RETRY_TINEOUT_MILLIS = LocalConfig.get(BusinessConstants.SysConfig.ES_MAX_RETRY_TINEOUT_MILLIS_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_MAX_RETRY_TINEOUT_MILLIS);
        ES_CONNECTION_REQUEST_TIMEOUT = LocalConfig.get(BusinessConstants.SysConfig.ES_CONNECTION_REQUEST_TIMEOUT_KEY, Integer.class, BusinessConstants.ESConfig.DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT);
    }

    private void closeESClient() throws SearchHandlerException {
        try {
            if (null != bulkProcessor) {
                boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
                if (terminated) {
                    if (null != restClient) {
                        restClient.close();
                    }

                    if (null != restHighLevelClient) {
                        restHighLevelClient.close();
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            String errMsg = "Error happened when we try to close ES client" + e;
            log.error(errMsg);
            throw new SearchHandlerException(ResultEnum.ES_CLIENT_CLOSE);
        }
    }
}
