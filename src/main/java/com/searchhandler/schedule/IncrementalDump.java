package com.searchhandler.schedule;

import com.searchhandler.service.DataTransportService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Component
@RestController
public class IncrementalDump {

    @Value("${dailyDump}")
    private boolean dailyDump = false;
    @Autowired
    private DataTransportService dataTransportService;

    @RequestMapping(value = "/dailyDump")
    @Scheduled(cron = "* * 2 * * ?")
    void dailyDump() {

        if (dailyDump) {
            log.info("Start daily incremental dump");
            long count = dataTransportService.dailyDump();
            log.info("Got {} incremental data in total", count);
        }
    }
}
