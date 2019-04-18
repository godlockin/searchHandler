package searchhandler.common;

import com.google.common.collect.Sets;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;

@Component
public class EnterpriseQueue {
    private static Set<String> baseSet = Sets.newConcurrentHashSet();
    private static Set<String> cacheSet = Sets.newConcurrentHashSet();
    private static Set<String> allEnterprise = Sets.newConcurrentHashSet();
    private static ConcurrentLinkedDeque<String> queue = new ConcurrentLinkedDeque<>();

    public static boolean recordBaseNames(String name) {
        return baseSet.add(name);
    }

    public static Set<String> loadBaseNames() {
        return baseSet;
    }

    public static void resetRecordNames() {
        cacheSet = Sets.newConcurrentHashSet();
    }

    public static boolean recordName(String name) {
        return cacheSet.add(name);
    }

    public static boolean recordNames(Collection<String> names) {
        return cacheSet.addAll(names);
    }

    public static boolean cacheEnterprise(String guid) {
         return allEnterprise.add(guid) && queue.add(guid);
    }

    public static String poll() {
        return queue.poll();
    }
}
