package com.tigerobo.searchhandler.common.constants;

public enum ResultEnum implements BaseEnum {

    SUCCESS(0, "成功"),
    FAILURE(1, "失败"),
    ES_CLIENT_INIT(11, "ES服务链接失败"),
    ES_CLIENT_BULK_COMMIT(12, "ES Bulk 提交失败"),
    ES_CLIENT_CLOSE(13, "ES Client 关闭失败"),
    ES_QUERY(14, "ES 检索失败"),
    PARAMETER_CHECK(21, "参数校验失败");

    private Integer code;
    private String message;

    ResultEnum(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public Integer getCode() {
        return this.code;
    }

    @Override
    public String getMessage() {
        return this.message;
    }
}
