package searchhandler.service;

import searchhandler.exception.SearchHandlerException;

import java.util.Map;

public interface EnterpriseService {

    String generateReportOnEnterprise(Map param) throws SearchHandlerException;

    Long generateEquity(Map param) throws SearchHandlerException;
}
