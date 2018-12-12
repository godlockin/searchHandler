package com.tigerobo.searchhandler.exception;

import com.tigerobo.searchhandler.common.constants.ResultEnum;
import lombok.Data;

@Data
public class SearchHandlerException extends Exception {

    private Integer code;

    public SearchHandlerException(ResultEnum result) {
        super(result.getMessage());
        this.code = result.getCode();
    }

    public SearchHandlerException(ResultEnum result, String message) {
        super(message);
        this.code = result.getCode();
    }

    public SearchHandlerException(Integer code, String msg) {
        super(msg);
        this.code = code;
    }
}
