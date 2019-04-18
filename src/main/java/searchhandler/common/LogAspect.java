package searchhandler.common;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.annotation.AfterReturning;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpServletRequest;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Aspect
@Component
public class LogAspect {

    private ConcurrentHashMap<Long, Long> cache = new ConcurrentHashMap<>();

    @Pointcut("execution(public * com.tigerobo.searchhandler.controller.*.*(..))")
    public void logic() {}

    @Before("logic()")
    public void doBeforeLogic(JoinPoint joinPoint) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        long threadId = Thread.currentThread().getId();
        long currTime = System.currentTimeMillis();
        log.info("Thread:[{}] Try to visit url:[{}] as method:[{}], which mapping to:[{}] with args:[{}] at:[{}]",
                threadId, request.getRequestURI(), request.getMethod(),
                joinPoint.getSignature().getDeclaringTypeName() + "," + joinPoint.getSignature().getName(),
                (null == joinPoint.getArgs()) ? "" : JSON.toJSON(joinPoint.getArgs()), currTime);
        cache.put(threadId, currTime);
    }

    @AfterReturning(returning = "object", pointcut = "logic()")
    public void doAfterLogic(Object object) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        HttpServletRequest request = attributes.getRequest();

        long threadId = Thread.currentThread().getId();
        long currTime = System.currentTimeMillis();
        long fromTime = Optional.ofNullable(cache.get(threadId)).orElse(0L);
        log.info("Thread:[{}] Finished visit url:[{}] as method:[{}] at:[{}] took:[{}]",
                threadId, request.getRequestURI(), request.getMethod(), currTime, (currTime - fromTime));
    }
}
