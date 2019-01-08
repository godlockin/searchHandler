package com.searchhandler.service;

import com.searchhandler.exception.SearchHandlerException;

public interface EnterpriseService {

    String buildData(String baseUrl, String trgtUrl) throws SearchHandlerException;
}
