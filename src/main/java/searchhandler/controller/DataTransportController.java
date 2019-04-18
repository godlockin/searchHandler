package searchhandler.controller;


import com.alibaba.fastjson.JSON;
import searchhandler.common.constants.BusinessConstants;
import searchhandler.exception.SearchHandlerException;
import searchhandler.model.Response;
import searchhandler.service.DataTransportService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping(value = "/data")
public class DataTransportController {

    @Autowired
    private DataTransportService dataTransportService;

    @RequestMapping(value = "/dailyDump", method = RequestMethod.POST)
    public Response<Long> dailyDump(@RequestBody Map param) {

        log.info("Start to dump incremental data for param:[{}]", JSON.toJSONString(param));
        Long dataNum = dataTransportService.dailyDump(param);
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/fullDump", method = RequestMethod.POST)
    public Response<Long> fullDumpData(@RequestBody Map param) {

        log.info("Start to dump all data for param:[{}]", JSON.toJSONString(param));
        Long dataNum = dataTransportService.fullDumpData(param);
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/provinceDump/{provinceCode}", method = RequestMethod.GET)
    public Response<Long> provinceDump(@PathVariable String provinceCode) {

        log.info("Start to dump data for province:[{}]", provinceCode);
        Long dataNum = dataTransportService.coreDump(new HashMap(){{put(BusinessConstants.QueryConfig.KEY_PROVINCE_CODE, provinceCode);}});
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/dataFix", method = RequestMethod.POST)
    public Response<Long> dataFix(@RequestBody Map param) {

        log.info("Start to fix data for config:[{}]", param);
        Long dataNum = dataTransportService.dataFix(param);
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/reindex", method = RequestMethod.POST)
    public Response<Long> reindex(@RequestBody Map param) throws SearchHandlerException {

        log.info("Start to do reindex for param:[{}]", JSON.toJSONString(param));
        Long dataNum = dataTransportService.reindex(param);
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }
}
