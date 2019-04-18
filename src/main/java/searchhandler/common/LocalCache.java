package searchhandler.common;

import searchhandler.common.constants.BusinessConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Component
public class LocalCache {
    private static ConcurrentHashMap<String, ConcurrentLinkedQueue<Map>> queueCache = new ConcurrentHashMap<>();

    public static void put(String code, Map config) {
        queueCache.get(code).add(config);
    }

    public static Map get(String code) {
        ConcurrentLinkedQueue<Map> queue = queueCache.get(code);
        return (null == queue || queue.isEmpty()) ? new HashMap() : queue.poll();
    }

    @PostConstruct
    void init() {
        BusinessConstants.PROVINCE_MAP.forEach((k, v) -> queueCache.put(k, new ConcurrentLinkedQueue<>()));
    }
}
