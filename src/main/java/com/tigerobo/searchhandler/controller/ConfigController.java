package com.tigerobo.searchhandler.controller;


import com.tigerobo.searchhandler.model.Response;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/config")
public class ConfigController {

    @RequestMapping(value = "/", method = RequestMethod.GET)
    public Response<List> listConfig() {

        return null;
    }
}
