package searchhandler.common.constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuppressWarnings({"unchecked"})
public class BusinessConstants {

    public static final String DELIMITER = "\001";

    public static class SysConfig {
        public SysConfig() {}

        public static final String ENV_FLG_KEY = "spring.profiles.active";
        public static final String BASE_CONFIG = "application.yml";
        public static final String CONFIG_TEMPLATE = "application-%s.yml";

        public static final String DAILY_DUMP_KEY = "dailyDump";
        public static final String ES_BULK_SIZE_KEY = "elasticsearch.bulk.size";
        public static final String ES_BULK_FLUSH_KEY = "elasticsearch.bulk.flush";
        public static final String ES_BULK_CONCURRENT_KEY = "elasticsearch.bulk.concurrent";
        public static final String ES_CONNECT_TIMEOUT_KEY = "elasticsearch.connect-timeout";
        public static final String ES_SOCKET_TIMEOUT_KEY = "elasticsearch.socket-timeout";
        public static final String ES_CONNECTION_REQUEST_TIMEOUT_KEY = "elasticsearch.connection-request-timeout";
        public static final String ES_MAX_RETRY_TINEOUT_MILLIS_KEY = "elasticsearch.max-retry-tineout-millis";

        public static final String ES_ADDRESSES_KEY = "elasticsearch.address";
        public static final String ES_HTTP_PORT_KEY = "elasticsearch.port.http";
        public static final String ES_INDEX_KEY = "elasticsearch.index";
        public static final String ES_TYPE_KEY = "elasticsearch.type";

        public static final String QUERY_PAGE_SIZE_KEY = "paging.pageSize";
        public static final String THREAD_POOL_SIZE_KEY = "threadpool.keep-alive-num";

        public static final String EQUITY_DEPTH_KEY = "equity.depth";
    }

    public static class QueryConfig {
        public QueryConfig() { }

        public static final int DEFAULT_THREAD_NUM = 1024;
        public static final int DEFAULT_PAGE_SIZE = 5000;
        public static final long DEFAULT_QUERY_INDEX = 0L;
        public static final String KEY_PROVINCE_CODE = "provinceCode";
        public static final String KEY_PROVINCE_NAME = "provinceName";
        public static final String KEY_PAGE_SIZE = "pageSize";
        public static final String KEY_ESINDEX = "esIndex";
        public static final String KEY_ESTYPE = "esType";
        public static final String KEY_QUERY_INDEX = "index";
        public static final String KEY_QUERY_TYPE = "type";
        public static final String KEY_QUERY_SIZE = "size";
        public static final String KEY_QUERY_FROM = "from";
        public static final String KEY_QUERY_IDLIST = "idList";

        public static final String KEY_QUERY = "query";
        public static final String KEY_TRGT_INDEX = "trgtIndex";
        public static final String KEY_TRGT_TYPE = "trgtType";
        public static final String DEFAULT_TYPE = "_doc";
    }

    public static final Map<String, String> PROVINCE_MAP = new HashMap() {{
        put("11", "北京");
        put("12", "天津");
        put("13", "河北");
        put("14", "山西");
        put("15", "内蒙古");
        put("21", "辽宁");
        put("22", "吉林");
        put("23", "黑龙江");
        put("31", "上海");
        put("32", "江苏");
        put("33", "浙江");
        put("34", "安徽");
        put("35", "福建");
        put("36", "江西");
        put("37", "山东");
        put("41", "河南");
        put("42", "湖北");
        put("43", "湖南");
        put("44", "广东");
        put("45", "广西");
        put("46", "海南");
        put("50", "重庆");
        put("51", "四川");
        put("52", "贵州");
        put("53", "云南");
        put("54", "西藏");
        put("61", "陕西");
        put("62", "甘肃");
        put("63", "青海");
        put("64", "宁夏");
        put("65", "新疆");
    }};

    public static class ESConfig {
        public ESConfig() {}

        public static final String DEFAULT_ES_ADDRESSES = "localhost";
        public static final int DEFAULT_ES_HTTP_PORT = 9200;
        public static final int DEFAULT_ES_BULK_SIZE = 10;
        public static final int DEFAULT_ES_BULK_FLUSH = 5000;
        public static final int DEFAULT_ES_BULK_CONCURRENT = 3;
        public static final int DEFAULT_ES_FROM = 0;
        public static final int DEFAULT_ES_SIZE = 10;
        public static final int DEFAULT_ES_MAX_SIZE = 10000;
        public static final int DEFAULT_ES_CONNECT_TIMEOUT = 5000;
        public static final int DEFAULT_ES_SOCKET_TIMEOUT = 40000;
        public static final int DEFAULT_ES_CONNECTION_REQUEST_TIMEOUT = 1000;
        public static final int DEFAULT_ES_MAX_RETRY_TINEOUT_MILLIS = 60000;
        public static final String DEFAULT_ES_INDEX = "business_data_current_reader";
        public static final String DEFAULT_ES_TYPE = "business_data";

        public static final String QUERY_KEY = "query";
        public static final String SCORE_KEY = "score";
        public static final String FILTER_KEY = "filter";
        public static final String HIGHLIGHT_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String FETCHSOURCE_KEY = "fetchSource";
        public static final String INCLUDE_KEY = "include";
        public static final String EXCLUDE_KEY = "exclude";
        public static final String SORT_KEY = "sort";

        public static final String SIMPLE_QUERY_NAME_KEY = "name";
        public static final String SIMPLE_QUERY_TYPE_KEY = "type";
        public static final String SIMPLE_QUERY_INDEX_KEY = "index";
        public static final String SIMPLE_QUERY_SIZE_KEY = "size";
        public static final String SIMPLE_QUERY_FIELD_KEY = "field";
        public static final String SIMPLE_QUERY_VALUE_KEY = "value";

        public static final String MUST_KEY = "must";
        public static final String SHOULD_KEY = "should";
        public static final String MUST_NOT_KEY = "must_not";

        public static final List<String> BOOL_CONDITION_LIST = Arrays.asList(MUST_KEY, SHOULD_KEY, MUST_NOT_KEY);

        public static final String TERM_KEY = "term";
        public static final String TERMS_KEY = "terms";
        public static final String MATCH_KEY = "match";
        public static final String FUZZY_KEY = "fuzzy";
        public static final String PREFIX_KEY = "prefix";
        public static final String REGEXP_KEY = "regexp";
        public static final String WRAPPER_KEY = "wrapper";
        public static final String WILDCARD_KEY = "wildcard";
        public static final String COMMONTERMS_KEY = "commonTerms";
        public static final String QUERY_STRING_KEY = "queryString";
        public static final String MATCH_PHRASE_KEY = "matchPhrase";
        public static final String MATCH_PHRASE_PREFIX_KEY = "matchPhrasePrefix";

        public static final List<String> SIMPLE_CONDITION_LIST = Arrays.asList(TERM_KEY, TERMS_KEY, MATCH_KEY,
                FUZZY_KEY, PREFIX_KEY, REGEXP_KEY, WRAPPER_KEY, WILDCARD_KEY, QUERY_STRING_KEY,
                MATCH_PHRASE_KEY, MATCH_PHRASE_PREFIX_KEY);

        public static final String RANGE_KEY = "range";

        public static final String INCLUDE_LOWER_KEY = "include_lower";
        public static final String INCLUDE_UPPER_KEY = "include_upper";
        public static final String FROM_KEY = "from";
        public static final String LTE_KEY = "lte";
        public static final String GTE_KEY = "gte";
        public static final String LT_KEY = "lt";
        public static final String GT_KEY = "gt";
        public static final String TO_KEY = "to";

        public static final String MULTIMATCH_KEY = "multiMatch";
        public static final String FIELDNAMES_KEY = "fieldNames";

        public static final String NESTED_KEY = "nested";
        public static final String PATH_KEY = "path";

        public static final String COUNT_KEY = "count";
        public static final String MAX_KEY = "max";
        public static final String MIN_KEY = "min";
        public static final String SUM_KEY = "sum";
        public static final String AVG_KEY = "avg";
        public static final String STATS_KEY = "stats";

        public static final String SHARD_SIZE_KEY = "shardSize";
        public static final String MISSING_KEY = "missing";
        public static final String MIN_DOC_COUNT_KEY = "minDocCount";
        public static final String SHARD_MIN_DOC_COUNT_KEY = "shardMinDocCount";

        public static final List<String> SIMPLE_AGGREGATION_LIST = Arrays.asList(COUNT_KEY, MAX_KEY, MIN_KEY,
                SUM_KEY, AVG_KEY, TERMS_KEY);

        public static final String SUB_AGG_KEY = "subAgg";
        public static final String DATE_RANGE_KEY = "dateRange";

        public static final String ANALYZER_KEY = "analyzer";
        public static final String CHAR_FILTER_KEY = "charFilter";
        public static final String TOKENIZER_KEY = "tokenizer";
        public static final String TOKEN_FILTER_KEY = "tokenFilter";
        public static final String INDEX_KEY = "index";
        public static final String FIELD_KEY = "field";
        public static final String NORMALIZER_KEY = "normalizer";

        public static final String DEFAULT_ANALYZER = "standard";

        public static final String SCROLL_TIME_VALUE_KEY = "timeValue";
        public static final String DEFAULT_SCROLL_TIME_VALUE = "1h";

        public static final String ORDER_KEY = "order";
        public static final String SORT_ORDER_ASC = "asc";
        public static final String SORT_ORDER_DESC = "desc";
        public static final String FIELD_SORT_TYPE = "field";
        public static final String SCRIPT_SORT_TYPE = "script";
        public static final String SCRIPT_TYPE = "scriptType";
        public static final String SCRIPT_LANG = "scriptLang";
        public static final String SCRIPT_SORT_SCRIPT_TYPE = "scriptSortType";
        public static final String SORT_MODE = "sortMode";
        public static final String SCRIPT_OPTIONS = "scriptOptions";
        public static final String SCRIPT_PARAMS = "scriptParams";

        public static final String NUMBER_TYPE = "number";
        public static final String STRING_TYPE = "string";

        public static final String MIN_MODE = "min";
        public static final String MAX_MODE = "max";
        public static final String SUM_MODE = "sum";
        public static final String AVG_MODE = "avg";
        public static final String MEDIAN_MODE = "median";

        public static final String INLINE_TYPE = "inline";
        public static final String STORED_TYPE = "stored";

        public static final String PAINLESS_TYPE = "painless";
    }

    public static class DataDumpConfig {
        public DataDumpConfig() {}

        public static final String ESSENTIAL_INFORMATION_KEY = "essential_information";
        public static final String KEY_PERSONNEL_KEY = "key_personnel";
        public static final String SHAREHOLDER_INFORMATION_KEY = "shareholder_information";

        public static final String ESSENTIAL_TIME_KEY = "essential_time";
        public static final String KEY_PERSONNEL_TIME_KEY = "key_personnel_time";
        public static final String SHAREHOLDER_INFORMATION_TIME_KEY = "shareholder_information_time";

        public static final String DATA_LIST_KEY = "data";
    }

    public static class ResultConfig {
        public ResultConfig() {}

        public static final String DATA_KEY = "data";
        public static final String TOTAL_KEY = "total";
        public static final String HIGHLIGH_KEY = "highlight";
        public static final String AGGREGATION_KEY = "aggregation";
        public static final String SCROLL_ID_KEY = "scrollId";
    }

    public static class BusinessDataConfig {
        public BusinessDataConfig() {}
        public static final List<String> ignoreList = Arrays.asList(
                  "enterprise_name_query"
                , "score"
                , "key_personnel"
                , "shareholder_information"
        );

        public static final List<String> baseKeyList = Arrays.asList(
                  "registration_status"
                , "registration_authority"
                , "charge_person"
                , "province_name"
                , "registered_capital_origin"
                , "business_scope"
                , "enterprises_type"
                , "enterprise_name"
        );

        public static final List<String> kpKeyList = Arrays.asList(
                  "name"
                , "position_chinese"
        );

        public static final List<String> siKeyList = Arrays.asList(
                  "shareholder_type"
                , "shareholder_credit_type"
                , "shareholder_name"
        );

        public static final String EQUITY_INDEX_KEY = "equityIndex";
        public static final String EQUITY_TYPE_KEY = "equityType";
        public static final String DEFAULT_EQUITY_INDEX = "equity_data_current_reader";
        public static final String DEFAULT_EQUITY_TYPE = "_doc";

    }
}
