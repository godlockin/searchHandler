package searchhandler.controller;

import com.alibaba.fastjson.JSON;
import searchhandler.exception.SearchHandlerException;
import searchhandler.model.Response;
import searchhandler.service.SearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping(value = "/search")
public class SearchController {
    @Autowired
    private SearchService searchService;

    @RequestMapping(value = "/search", method = RequestMethod.POST)
    public Response<Map> searchData(@RequestBody Map param) throws SearchHandlerException {

        log.info("Do simple search for param:[{}]", JSON.toJSONString(param));
        Map result = searchService.doSimpleSearch(param);
        return new Response<>(result);
    }

    @RequestMapping(value = "/complexSearch", method = RequestMethod.POST)
    public Response<Map> complexSearchData(@RequestBody Map param) throws SearchHandlerException {

        log.info("Do complex search for param:[{}]", JSON.toJSONString(param));
        Map result = searchService.doComplexSearch(param);
        return new Response<>(result);
    }

    @RequestMapping(value = "/analyze", method = RequestMethod.POST)
    public Response<Map> analyze(@RequestBody Map param) throws SearchHandlerException {

        log.info("Do text analyze for param:[{}]", JSON.toJSONString(param));
        Map result = searchService.doAnalyze(param);
        return new Response<>(result);
    }
}
