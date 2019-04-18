package searchhandler.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import searchhandler.common.constants.ResultEnum;
import searchhandler.exception.SearchHandlerException;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.ByteArrayHttpMessageConverter;
import org.springframework.http.converter.ResourceHttpMessageConverter;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter;
import org.springframework.http.converter.xml.SourceHttpMessageConverter;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Map;

@Slf4j
@Component
public class RestHttpClient {

    private static CloseableHttpClient closeableHttpClient;
    private static RequestConfig requestConfig;

    @Autowired
    private CloseableHttpClient httpClient;

    @Autowired
    private RequestConfig config;

    private static RestTemplate restTemplate = new RestTemplate(Arrays.asList(
              new StringHttpMessageConverter(Charset.forName("UTF-8"))
            , new ByteArrayHttpMessageConverter()
            , new ResourceHttpMessageConverter()
            , new SourceHttpMessageConverter<>()
            , new FastJsonHttpMessageConverter()
            , new Jaxb2RootElementHttpMessageConverter()));

    @PostConstruct
    void init() {
        closeableHttpClient = httpClient;
        requestConfig = config;
    }

    public static String doPost(String url, Map<String, ?> param) throws SearchHandlerException {
        DataUtils.multiCheck(true, "Can't find url info", url);
        log.debug("Try to post url:[{}] with param:[{}]", url, JSON.toJSONString(param));

        HttpHeaders requestHeaders = new HttpHeaders();
        requestHeaders.setContentType(MediaType.APPLICATION_JSON_UTF8);
        HttpEntity<Map> httpEntity = new HttpEntity<>(param, requestHeaders);
        ResponseEntity<String> result = restTemplate.postForEntity(url, httpEntity, String.class);
        DataUtils.multiCheck(false, "Can't get result", result);
        log.debug("Got result:[{}]", result);
        return result.getBody();
    }

    public static String doGet(String url) throws SearchHandlerException {
        DataUtils.multiCheck(true, "Can't find url info", url);
        log.info("Try to get url:[{}]", url);
        return doHttpCall(new HttpGet(url));
    }

    private static String doHttpCall(HttpRequestBase requestBase) throws SearchHandlerException {

        // 装载配置信息
        requestBase.setConfig(requestConfig);
        try (CloseableHttpResponse response = closeableHttpClient.execute(requestBase)) {

            // 判断状态码是否为200
            if (response.getStatusLine().getStatusCode() == 200) {
                // 返回响应体的内容
                String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                log.debug("Get result:[{}]", result);
                return result;
            }

            return null;
        } catch (Exception e) {
            e.printStackTrace();
            log.error("Error happened when we do http call for:[{}], {}", requestBase.getURI(), e);
            throw new SearchHandlerException(ResultEnum.FAILURE);
        }
    }
}
