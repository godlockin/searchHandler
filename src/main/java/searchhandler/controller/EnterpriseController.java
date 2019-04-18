package searchhandler.controller;


import searchhandler.service.EnterpriseService;
import searchhandler.exception.SearchHandlerException;
import searchhandler.model.Response;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping(value = "/enterprise")
public class EnterpriseController {

    @Autowired
    private EnterpriseService enterpriseService;

    @RequestMapping(method = RequestMethod.POST)
    public Response<String> getEnterPriseInfo(@RequestBody Map param) throws SearchHandlerException {

        String result = enterpriseService.generateReportOnEnterprise(param);
        return new Response<>(result);
    }

    @RequestMapping(value = "/equity", method = RequestMethod.POST)
    public Response<Long> equity(@RequestBody Map param) throws SearchHandlerException {

        Long result = enterpriseService.generateEquity(param);
        return new Response<>(result);
    }
}
