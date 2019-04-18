package searchhandler.common.component;

import com.google.common.collect.Sets;
import searchhandler.common.utils.GuidService;
import searchhandler.common.utils.RedisUtil;
import searchhandler.service.ESService;
import searchhandler.common.utils.DataUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
@Component
@SuppressWarnings({"unchecked"})
public class FullDetailsGenerateCallable implements Callable<List> {

    private List dataList;
    private Integer equityDeep;
    private ESService esService;
    private String index;
    private String type;

    public FullDetailsGenerateCallable(ESService esService, List dataList) {

        this.esService = esService;
        this.dataList = dataList;
    }

    @Override
    public List call() {
        log.info("Start to cache {} enterprise info", dataList.size());
        if (dataList.isEmpty()) {
            log.error("No data found");
            return new ArrayList();
        }

        List result = (List) dataList.parallelStream().filter(x -> !CollectionUtils.isEmpty((Map) x))
                        .map(x -> DataUtils.getNotNullValue((Map) x, "enterprise_name", String.class, "").trim())
                        .filter(x -> StringUtils.isNotBlank((String) x))
                        .map(getListGenerator()).filter(x -> !((Map) x).isEmpty()).collect(Collectors.toList());
        esService.bulkInsert(index, type, "guid", result);
        log.info("Build as {} enterprise info", result.size());
        return result;
    }

    private Function<Object, Map> getListGenerator() {
        return x -> {
            Map trgt = new HashMap();
            // ignored companies with duplicate name here
            String guid = GuidService.getGuid((String) x);
            if (RedisUtil.exists(guid)) {
                Set<String> distinct = Sets.newConcurrentHashSet();
                trgt = buildEquity(distinct, 0).apply(RedisUtil.hgetAll(guid));
            } else {
                log.error("enterprise_name:[{}], guid:[{}] not found", x, guid);
            }
            return trgt;
        };
    }

    private Function<Map, Map> buildEquity(Set<String> distinct, final int level) {
        return data -> {
            String guid = DataUtils.getNotNullValue(data, "guid", String.class, "");
            data.put("level", level);
            int nextLevel = level + 1;
            if (distinct.add(guid)) {
                data.put("shareholder", handleDeeperNode(guid, "_core", distinct, nextLevel));
                data.put("investment", handleDeeperNode(guid, "_si", distinct, nextLevel));
            }
            return data;
        };
    }

    private List handleDeeperNode(String key, String appendKey, Set<String> distinct, int currLevel) {
        List result = Optional.ofNullable(RedisUtil.smembers(key + appendKey)).orElse(new HashSet<>())
                .stream()
                .map(RedisUtil::hgetAll)
                .filter(x -> !(CollectionUtils.isEmpty(x)))
                .map(buildEquity(distinct, currLevel)::apply)
                .collect(Collectors.toList());
        return (CollectionUtils.isEmpty(result)) ? new ArrayList() : result;
    }
}
