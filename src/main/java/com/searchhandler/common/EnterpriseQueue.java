package com.searchhandler.common;

import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

@Component
public class EnterpriseQueue {
    private static Set<String> baseSet = new HashSet<>();
    private static Set<String> cacheSet = new HashSet<>();
    private static ConcurrentLinkedQueue<List> queue = new ConcurrentLinkedQueue<>();
    private static ConcurrentLinkedQueue<List> tmpName = new ConcurrentLinkedQueue<>();

    public static boolean recordBaseNames(String name) {
        return baseSet.add(name);
    }

    public static Set<String> loadBaseNames() {
        return baseSet;
    }

    public static boolean recordName(String name) {
        return cacheSet.add(name);
    }

    public static boolean recordNames(Collection<String> names) {
        return cacheSet.addAll(names);
    }

    public static Set<String> getAllNames() {
        return cacheSet;
    }

    public static boolean pushData(List dataList) {
        return queue.add(dataList);
    }

    public static List pollData() {

        return Optional.ofNullable(queue.poll()).orElse(new ArrayList());
    }

    public static boolean additionalNames(List names) {
        return tmpName.add(names);
    }

    public static List getAdditionalNames() {
        return Optional.ofNullable(tmpName.poll()).orElse(new ArrayList());
    }
}
