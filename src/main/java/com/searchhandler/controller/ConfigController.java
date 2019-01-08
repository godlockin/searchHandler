package com.searchhandler.controller;


import com.searchhandler.exception.SearchHandlerException;
import com.searchhandler.model.Response;
import com.searchhandler.model.Response;
import com.searchhandler.service.ConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping(value = "/config")
public class ConfigController {

    @Autowired
    private ConfigService configService;

    @RequestMapping(method = RequestMethod.GET)
    public Response<Map> getConfig() {

        return new Response<>(configService.listConfig());
    }

    @RequestMapping(method = RequestMethod.POST)
    public Response<Map> updConfig(@RequestBody Map<String, Object> config) throws SearchHandlerException {

        return new Response<>(configService.updConfig(config));
    }
}
