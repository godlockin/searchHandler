package searchhandler.common.component;

import searchhandler.common.EnterpriseQueue;
import searchhandler.common.utils.GuidService;
import searchhandler.common.utils.RedisUtil;
import searchhandler.common.utils.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

@Slf4j
@Component
@SuppressWarnings({"unchecked"})
public class FullDetailsCacheCallable implements Callable<List> {

    protected List dataList;
    protected Set<String> additionalSet = new HashSet<>();
    private Consumer<Object> nextListGenerator = getListGenerator();

    public FullDetailsCacheCallable(List dataList) {
        this.dataList = dataList;
    }

    @Override
    public List call() {
        log.info("Start to cache {} enterprise info", dataList.size());
        if (dataList.isEmpty()) {
            log.error("No data found");
            return new ArrayList();
        }

        dataList.forEach(nextListGenerator);
        log.info("Recorded {} company info and {} addition info", dataList.size(), additionalSet.size());
        return new ArrayList(additionalSet);
    }

    protected Consumer<Object> getListGenerator() {
        return (x) -> {
            Map data = (Map) x;
            if (CollectionUtils.isEmpty(data)) {
                return;
            }

            String enterprise_name = DataUtils.getNotNullValue(data, "enterprise_name", String.class, "").trim();
            if (StringUtils.isBlank(enterprise_name)) {
                return;
            }

            // TODO
            // ignored companies with duplicate name here
            String guid = GuidService.getGuid(enterprise_name);
            if (!EnterpriseQueue.cacheEnterprise(guid)) {
                return;
            }

            // cached basic info
            Map trgt = new HashMap() {{
                put("enterprise_name", enterprise_name);
                put("registration_status", DataUtils.getNotNullValue(data, "registration_status", String.class, ""));
                put("charge_person", DataUtils.getNotNullValue(data, "charge_person", String.class, ""));
                put("province_name", DataUtils.getNotNullValue(data, "province_name", String.class, ""));
                put("registered_capital_origin", DataUtils.getNotNullValue(data, "registered_capital_origin", String.class, ""));
                put("guid", guid);
            }};
            RedisUtil.hmset(guid, trgt);

            // cache shareholder info
            List<Map> share = DataUtils.getNotNullValue(data, "shareholder_information", List.class, new ArrayList<>());
            if (share.isEmpty()) {
                return;
            }

            String[] shareInfo = share.stream().map(y -> {
                String shareholder_type = DataUtils.getNotNullValue(y, "shareholder_type", String.class, "");
                if (!"自然人股东".equals(shareholder_type)) {
                    String shareholder_name = DataUtils.getNotNullValue(y, "shareholder_name", String.class, "").trim();
                    if (StringUtils.isNotBlank(shareholder_name) && additionalSet.add(shareholder_name)) {
                        String siId = GuidService.getGuid(shareholder_name);
                        if (0L < RedisUtil.sadd(siId + "_si", guid)) {
                            return siId;
                        }
                    }
                }
                return "";
            }).filter(StringUtils::isNotBlank).toArray(String[]::new);
            int shSize = shareInfo.length;
            if (0 < shSize) {
                log.debug("Add guid:[{}], {} sh info", guid, shSize);
                RedisUtil.sadd(guid + "_core", shareInfo);
            }
        };
    }
}
