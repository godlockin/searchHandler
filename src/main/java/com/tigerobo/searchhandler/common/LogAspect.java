package com.tigerobo.searchhandler.common;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@Aspect
@Component
public class LogAspect {

    @Pointcut("execution(public * com.tigerobo.searchhandler.controller.*.*(..))")
    public void logic() {}

    @Before("logic()")
    public void doBeforeLogic(JoinPoint joinPoint) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        log.info("Try to visit url:[{}] as method:[{}], which mapping to:[{}] with args:[{}]",
                request.getRequestURI(), request.getMethod(),
                joinPoint.getSignature().getDeclaringTypeName() + "," + joinPoint.getSignature().getName(),
                joinPoint.getArgs());
    }

    @AfterReturning(returning = "object", pointcut = "logic()")
    public void doAfterLogic(Object object) {
        log.info("Return as:[{}]", object);
    }
}
