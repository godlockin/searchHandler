package com.searchhandler.common;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EnterpriseCache {
    private static ConcurrentHashMap<String, Map> cache = new ConcurrentHashMap<>();
    private static ConcurrentHashMap<String, Set<String>> shareHolderCache = new ConcurrentHashMap<>();

    synchronized public static void put(String key, Map company) {
        if (!cache.containsKey(key)) {
            cache.put(key, company);
        } else {
            cache.get(key).putAll(company);
        }
    }

    public static Map get(String key) {
        return Optional.ofNullable(cache.get(key)).orElse(new HashMap());
    }

    public static Map get() {
        return cache;
    }

    synchronized public static void append(String key, String holder) {
        if (!shareHolderCache.containsKey(key)) {
            shareHolderCache.put(key, new HashSet(Collections.singletonList(holder)));
        } else {
            shareHolderCache.get(key).add(holder);
        }
    }

    public static List<String> pop(String key) {
        return new ArrayList(Optional.ofNullable(shareHolderCache.get(key)).orElse(new HashSet<>()));
    }
}
