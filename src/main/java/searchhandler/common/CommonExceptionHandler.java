package searchhandler.common;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import searchhandler.common.constants.ResultEnum;
import searchhandler.exception.SearchHandlerException;
import searchhandler.model.Response;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;

@Slf4j
@Component
@ControllerAdvice
public class CommonExceptionHandler {

    @ExceptionHandler(value = Exception.class)
    @ResponseBody
    public Response exceptionHandle(HttpServletRequest request, Exception e) {
        e.printStackTrace();
        log.error("Error happened on url:[{}]", request.getRequestURI());
        if (e instanceof SearchHandlerException) {
            SearchHandlerException se = (SearchHandlerException) e;
            return new Response(se.getCode(), se.getMessage(), new HashMap<>());
        }
        return new Response(ResultEnum.FAILURE);
    }
}
