package searchhandler.common.component;


import searchhandler.common.utils.GuidService;
import searchhandler.service.ESService;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.stream.Collectors;

@Data
@Slf4j
@SuppressWarnings({ "unchecked" })
public class DataFixMonitorCallable implements Callable<Long> {

    private Map param;
    private List dataList;
    private ESService esService;
    private Function<Object, Map> itemFixFunction = getItemFixFunction();

    public DataFixMonitorCallable(Map param, List dataList, ESService esService) {
        this.param = param;
        this.dataList = dataList;
        this.esService = esService;
    }

    @Override
    public Long call() {
        if (CollectionUtils.isEmpty(dataList)) {
            log.error("No data found");
            return 0L;
        }

        log.info("Start to fix {} data", dataList.size());
        String trgtIndex = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_TRGT_INDEX, String.class, "");
        String trgtType = DataUtils.getNotNullValue(param, BusinessConstants.QueryConfig.KEY_TRGT_TYPE, String.class, BusinessConstants.ESConfig.DEFAULT_ES_TYPE);
        List trgt = (List) dataList.stream().map(itemFixFunction).filter(x -> existCheckFunction("enterprise_name").apply(x)).collect(Collectors.toList());
        Long count = esService.bulkInsert(trgtIndex, trgtType, "id", trgt).longValue();
        log.info("Fixed {} data", count);
        return count;
    }

    private Function<Object, Boolean> existCheckFunction(String key) {
        return base -> !CollectionUtils.isEmpty((Map) base) &&
                StringUtils.isNotBlank(DataUtils.getNotNullValue((Map) base, key, String.class, ""));
    }

    private Function<Object, Map> getItemFixFunction() {
        return base -> {
            Map trgt = new HashMap();
            if (null == base || CollectionUtils.isEmpty((Map) base)) {
                return trgt;
            }

            Map<String, Object> data = (Map) base;
            String enterprise_name = DataUtils.getNotNullValue(data, "enterprise_name", String.class, "");
            if (StringUtils.isBlank(enterprise_name)) {
                return trgt;
            }

            List baseList = handleSubList(BusinessConstants.BusinessDataConfig.ignoreList, BusinessConstants.BusinessDataConfig.baseKeyList).apply(Collections.singletonList(data));
            if (CollectionUtils.isEmpty(baseList)) {
                return trgt;
            }

            trgt = (Map) baseList.get(0);
            trgt.put("guid", GuidService.getGuid(enterprise_name).trim());

            List kp = DataUtils.getNotNullValue(data, "key_personnel", List.class, new ArrayList<>());
            trgt.put("key_personnel", handleSubList(new ArrayList<>(), BusinessConstants.BusinessDataConfig.kpKeyList).apply(kp));

            List si = DataUtils.getNotNullValue(data, "shareholder_information", List.class, new ArrayList<>());
            trgt.put("shareholder_information", handleSubList(new ArrayList<>(), BusinessConstants.BusinessDataConfig.siKeyList).apply(si));

            return trgt;
        };
    }

    private Function<List, List> handleSubList(List<String> ignoreList, List<String> convertList) {
        return list -> {
            Set<Integer> set = new HashSet<>();
            return (List) Optional.ofNullable(list).orElse(new ArrayList())
                    .stream().filter(x -> distinct(set).apply(x))
                    .map(x -> {
                        Map<String, Object> data = (Map) x;
                        Map trgt = new HashMap();
                        data.entrySet().stream().filter(y -> !ignoreList.contains(y.getKey()))
                                .peek(y -> y.setValue((convertList.contains(y.getKey())) ? handleFullWidth(y.getKey(), data) : y.getValue()))
                                .forEach(y -> trgt.put(y.getKey(), y.getValue()));
                        return trgt;
                    }).filter(x -> !CollectionUtils.isEmpty((Map) x)).collect(Collectors.toList());
        };
    }

    private Function<Object, Boolean> distinct(Set<Integer> set) {
        return (base) -> {
            int serialno = DataUtils.getNotNullValue((Map) base, "serialno", Integer.class, 0);
            return (0 != serialno) && set.add(serialno);
        };
    }

    private String handleFullWidth(String key, Map data) {
        return DataUtils.fullWidth2halfWidth(DataUtils.getNotNullValue(data, key, String.class, "").trim());
    }
}
