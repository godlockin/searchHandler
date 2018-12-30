package com.searchhandler.service;

import java.util.Map;

public interface DataTransportService {
    Long fullDumpData();

    Long coreDump(Map<String, Object>  config);

    Long dailyDump();
}
