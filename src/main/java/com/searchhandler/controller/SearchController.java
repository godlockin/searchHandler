package com.searchhandler.controller;

import com.searchhandler.common.constants.BusinessConstants;
import com.searchhandler.model.Response;
import com.searchhandler.service.DataTransportService;
import com.searchhandler.service.SearchService;
import com.searchhandler.exception.SearchHandlerException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping(value = "/search")
public class SearchController {

    @Autowired
    private DataTransportService dataTransportService;
    @Autowired
    private SearchService searchService;

    @RequestMapping(value = "/dailyDump", method = RequestMethod.GET)
    public Response<Long> dailyDump() {

        log.info("Start to dump incremental data");
        Long dataNum = dataTransportService.dailyDump();
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/fullDump", method = RequestMethod.GET)
    public Response<Long> fullDumpData() {

        log.info("Start to dump all data");
        Long dataNum = dataTransportService.fullDumpData();
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

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public Response<Map> searchData(@RequestBody Map param) throws SearchHandlerException {

        Map result = searchService.doSimpleSearch(param);
        return new Response<>(result);
    }

    @RequestMapping(value = "/complexSearch", method = RequestMethod.POST)
    public Response<Map> complexSearchData(@RequestBody Map param) throws SearchHandlerException {

        Map result = searchService.doComplexSearch(param);
        return new Response<>(result);
    }

    @RequestMapping(value = "/analyze", method = RequestMethod.POST)
    public Response<Map> analyze(@RequestBody Map param) throws SearchHandlerException {

        Map result = searchService.doAnalyze(param);
        return new Response<>(result);
    }
}
