package com.tigerobo.searchhandler.common.constants;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings({"unchecked"})
public class BusinessConstants {


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
        public static final String KEY_QUERY_SIZE = "size";
        public static final String KEY_QUERY_IDLIST = "idList";
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

    }
}
