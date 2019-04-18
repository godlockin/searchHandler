package searchhandler.model;

import searchhandler.common.constants.ResultEnum;
import lombok.Data;

import java.util.HashMap;

@Data
public class Response<T> {

    private Integer code = ResultEnum.SUCCESS.getCode();
    private String message = ResultEnum.SUCCESS.getMessage();
    private T data = (T) new HashMap<>();

    public Response() {}

    public Response(T data) {
        this.data = data;
    }

    public Response(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    public Response(ResultEnum resultEnum) {
        this.code = resultEnum.getCode();
        this.message = resultEnum.getMessage();
        this.data = (T) new HashMap();
    }
}
