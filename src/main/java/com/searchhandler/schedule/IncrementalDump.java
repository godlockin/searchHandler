package com.searchhandler.schedule;

import com.searchhandler.common.LocalConfig;
import com.searchhandler.service.DataTransportService;
import com.searchhandler.common.constants.BusinessConstants;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Component
@RestController
@EnableScheduling
public class IncrementalDump {

    @Autowired
    private DataTransportService dataTransportService;

    @RequestMapping(value = "/dailyDump", method = RequestMethod.GET)
    @Scheduled(cron = "* * 2 * * *")
    void dailyDump() {
        if (LocalConfig.get(BusinessConstants.SysConfig.DAILY_DUMP_KEY, Boolean.class, false)) {
            log.info("Start daily incremental dump");
            long count = dataTransportService.dailyDump();
            log.info("Got {} incremental data in total", count);
        }
    }
}
