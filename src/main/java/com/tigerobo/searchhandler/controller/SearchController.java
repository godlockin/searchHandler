package com.tigerobo.searchhandler.controller;

import com.tigerobo.searchhandler.exception.SearchHandlerException;
import com.tigerobo.searchhandler.model.Response;
import com.tigerobo.searchhandler.service.DataTransportService;
import com.tigerobo.searchhandler.service.SearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping(value = "/search")
public class SearchController {

    @Autowired
    private DataTransportService dataTransportService;
    @Autowired
    private SearchService searchService;

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
        Long dataNum = dataTransportService.provinceDump(provinceCode);
        log.info("Handled {} data", dataNum);
        return new Response<>(dataNum);
    }

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public Response<Map> searchData(@RequestBody Map param) throws SearchHandlerException {

        Map result = searchService.doSimpleSearch(param);
        return new Response<>(result);
    }
}
