package com.tigerobo.searchhandler.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    @RequestMapping(value = "/health")
    public int healthCheck() {
        return 200;
    }
}
