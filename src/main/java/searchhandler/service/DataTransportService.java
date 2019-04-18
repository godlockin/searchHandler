package searchhandler.service;

import searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface DataTransportService {
    Long fullDumpData(Map<String, Object> config);

    Long coreDump(Map<String, Object> config);

    Long dailyDump(Map<String, Object> config);

    Long reindex(Map param) throws SearchHandlerException;

    Long dataDumpForCNo();

    Long dataFix(Map param);

    long handleBaseDataList(Map config);
}
