package com.tigerobo.searchhandler.dao.impl;

import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.session.SqlSessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class BaseDaoImpl {

    @Autowired
    protected SqlSessionFactory sqlSessionFactory;

    public List notNullList(List list) {

        return CollectionUtils.isEmpty(list) ? new ArrayList() : list;
    }

    protected void printSql(String method, Object param) {
        String sql = sqlSessionFactory.getConfiguration().getMappedStatement(method).getBoundSql(param).getSql();
        log.info("Execute sql:\n{}", sql);
    }
}
