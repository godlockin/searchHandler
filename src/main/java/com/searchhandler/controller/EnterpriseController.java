package com.searchhandler.controller;


import com.searchhandler.common.utils.DataUtils;
import com.searchhandler.exception.SearchHandlerException;
import com.searchhandler.model.Response;
import com.searchhandler.service.EnterpriseService;
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

    @RequestMapping(value = "", method = RequestMethod.POST)
    public Response<String> getEnterPriseInfo(@RequestBody Map param) throws SearchHandlerException {

        String baseUrl = DataUtils.getNotNullValue(param, "baseUrl", String.class, "");
        String trgtUrl = DataUtils.getNotNullValue(param, "trgtUrl", String.class, "");

        String result = enterpriseService.buildData(baseUrl, trgtUrl);
        return new Response<>(result);
    }
}
