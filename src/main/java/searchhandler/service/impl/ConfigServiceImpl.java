package searchhandler.service.impl;

import searchhandler.exception.SearchHandlerException;
import searchhandler.common.LocalConfig;
import searchhandler.service.ConfigService;
import searchhandler.service.ESService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Slf4j
@Service
public class ConfigServiceImpl implements ConfigService {

    @Autowired
    private ESService esService;

    @Override
    public Map listConfig() {

        return LocalConfig.get();
    }

    @Override
    public Map updConfig(Map<String, Object> config) throws SearchHandlerException {

        LocalConfig.putAll(config);
        String esConfig = config.entrySet().parallelStream().filter((x) -> x.getKey().startsWith("elasticsearch"))
                .map(Map.Entry::getKey).findFirst().orElse("");
        if (StringUtils.isNotBlank(esConfig)) {
            esService.initESClient();
        }

        return LocalConfig.get();
    }
}
