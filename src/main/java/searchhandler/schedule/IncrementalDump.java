package searchhandler.schedule;

import searchhandler.common.LocalConfig;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.service.DataTransportService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@Component
@RestController
@EnableScheduling
public class IncrementalDump {

    @Autowired
    private DataTransportService dataTransportService;

    @RequestMapping(value = "/dailyDump")
    @Scheduled(cron = "10 * * * * *")
    void dailyDump() {
        if (LocalConfig.get(BusinessConstants.SysConfig.DAILY_DUMP_KEY, Boolean.class, false)) {
            log.info("Start daily incremental dump");
//            long count = dataTransportService.dailyDump(new HashMap<>());
            long count = dataTransportService.dataDumpForCNo();
            log.info("Got {} incremental data in total", count);
        }
    }
}
