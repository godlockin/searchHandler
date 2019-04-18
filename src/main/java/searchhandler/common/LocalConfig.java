package searchhandler.common;

import searchhandler.common.constants.BusinessConstants;
import searchhandler.common.utils.DataUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;
import org.yaml.snakeyaml.Yaml;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class LocalConfig {

    private static ConcurrentHashMap<String, Object> cache = new ConcurrentHashMap<>();

    public static Object put(String k, Object v) { return cache.put(k, v); }

    public static void putAll(Map<String, Object> config) { cache.putAll(config); }

    public static <T> T get(String k, Class clazz, Object defaultValue) {
        if (cache.isEmpty()) {
            synchronized (LocalConfig.class) {
                if (cache.isEmpty()) {
                    sysInit();
                }
            }
        }
        return (T) DataUtils.getNotNullValue(cache, k, clazz, defaultValue);
    }

    public static Map get() {
        return new HashMap(cache);
    }

    @PostConstruct
    public static void sysInit() {
        // cache base config
        Map baseConfig = loadYamlConfig(BusinessConstants.SysConfig.BASE_CONFIG);

        // load config for env
        String envFlg = DataUtils.getNotNullValue(baseConfig, BusinessConstants.SysConfig.ENV_FLG_KEY, String.class, "");
        if (StringUtils.isNotBlank(envFlg)) {
            putAll(loadYamlConfig(String.format(BusinessConstants.SysConfig.CONFIG_TEMPLATE, envFlg)));
        }
    }

    private static Map<String, Object> loadYamlConfig(String fileName) {

        Map trgt = new HashMap();
        try (InputStream fis = LocalConfig.class.getClassLoader().getResourceAsStream(fileName)) {
            Map base = new Yaml().load(fis);
            flattenMap("", base, trgt);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return trgt;
    }

    private static void flattenMap(String prefixKey, Map base, Map trgt) {

        prefixKey = (StringUtils.isNotBlank(prefixKey)) ? prefixKey + "." : "";
        String key = prefixKey;
        base.forEach((k, v) -> {
            if (v instanceof Map) {
                flattenMap(key + k, (Map) v, trgt);
            } else {
                trgt.put(key + k, v);
            }
        });
    }
}
