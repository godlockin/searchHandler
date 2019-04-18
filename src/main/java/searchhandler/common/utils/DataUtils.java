package searchhandler.common.utils;

import searchhandler.common.constants.ResultEnum;
import searchhandler.exception.SearchHandlerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class DataUtils {

    public static final String DATE_YYYYMMDD_HHMMSS = "yyyy-MM-dd HH:mm:ss";
    public static final String DEFAULT_DATE_FORMAT = DATE_YYYYMMDD_HHMMSS;

    public static List splitWithoutEmpty(String baseStr, String splitKey) {
        return Optional.ofNullable(baseStr).map(x ->
                Stream.of(x.trim().split(splitKey))
                        .map(String::trim)
                        .filter(StringUtils::isNotBlank)
                        .collect(Collectors.toList()))
                .orElse(new ArrayList());
    }

    public static void multiCheck(boolean throwAble, String errMsg, Object...objects) throws SearchHandlerException {
        if (null == objects || 0 == objects.length) {
            handleFailure(throwAble, errMsg);
            return;
        }

        for (Object object : objects) {
            if (null == object) {
                handleFailure(throwAble, errMsg);
            } else if (object instanceof String) {
                if (StringUtils.isBlank(DataUtils.handleNullValue(object, String.class, ""))) {
                    handleFailure(throwAble, errMsg);
                }
            }
        }
    }

    private static void handleFailure(boolean throwAble, String errMsg) throws SearchHandlerException {
        if (throwAble) {
            throw new SearchHandlerException(ResultEnum.PARAMETER_CHECK, errMsg);
        } else {
            log.error(errMsg);
        }
    }

    public static <T> T getNotNullValue(Map base, String key, Class<T> clazz, Object defaultValue) {
        return handleNullValue(base.get(key), clazz, defaultValue);
    }

    public static <T> T handleNullValue(Object base, Class<T> clazz, Object defaultValue) {
        return clazz.cast(Optional.ofNullable(base).orElse(defaultValue));
    }

    public static String fullWidth2halfWidth(String fullWidthStr) {
        if (StringUtils.isBlank(fullWidthStr)) {
            return "";
        }

        char[] charArray = fullWidthStr.toCharArray();
        //对全角字符转换的char数组遍历
        for (int i = 0; i < charArray.length; ++i) {
            int charIntValue = (int) charArray[i];
            //如果符合转换关系,将对应下标之间减掉偏移量65248;如果是空格的话,直接做转换
            if (charIntValue >= 65281 && charIntValue <= 65374) {
                charArray[i] = (char) (charIntValue - 65248);
            } else if (charIntValue == 12288) {
                charArray[i] = (char) 32;
            }
        }
        return new String(charArray);
    }

    public static <E> void forEach(Integer maxIndex, Iterable<? extends E> elements, BiConsumer<Integer, ? super E> action) {
        Objects.requireNonNull(elements);
        Objects.requireNonNull(action);
        int index = 0;
        for (E element : elements) {
            action.accept(index++, element);
            if (maxIndex > 0 && maxIndex < index) {
                break;
            }
        }
    }
}
