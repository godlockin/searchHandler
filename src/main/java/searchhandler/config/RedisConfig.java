package searchhandler.config;

import searchhandler.common.utils.RedisPool;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

@Configuration
public class RedisConfig {
	
    @Value("${spring.redis.maxIdle}")
    private Integer maxIdle;

    @Value("${spring.redis.maxTotal}")
    private Integer maxTotal;

    @Value("${spring.redis.maxWaitMillis}")
    private Integer maxWaitMillis;

    @Value("${spring.redis.minEvictableIdleTimeMillis}")
    private Integer minEvictableIdleTimeMillis;

    @Value("${spring.redis.numTestsPerEvictionRun}")
    private Integer numTestsPerEvictionRun;

    @Value("${spring.redis.timeBetweenEvictionRunsMillis}")
    private long timeBetweenEvictionRunsMillis;

    @Value("${spring.redis.testOnBorrow}")
    private boolean testOnBorrow;

    @Value("${spring.redis.testWhileIdle}")
    private boolean testWhileIdle;
    
    @Value("${spring.redis.database}")
    private Integer database;
    
    @Value("${spring.redis.host}")
    private String host;
    
    @Value("${spring.redis.port}")
    private Integer port;
    
    @Value("${spring.redis.password}")
    private String password;
    
    private static JedisPool pool = null;

    /**
     * JedisPoolConfig 连接池
     * @return
     */
    @Bean
    public JedisPoolConfig jedisPoolConfig() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        // 最大空闲数
        jedisPoolConfig.setMaxIdle(maxIdle);
        // 连接池的最大数据库连接数
        jedisPoolConfig.setMaxTotal(maxTotal);
        // 最大建立连接等待时间
        jedisPoolConfig.setMaxWaitMillis(maxWaitMillis);
        // 逐出连接的最小空闲时间 默认1800000毫秒(30分钟)
        jedisPoolConfig.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        // 每次逐出检查时 逐出的最大数目 如果为负数就是 : 1/abs(n), 默认3
        jedisPoolConfig.setNumTestsPerEvictionRun(numTestsPerEvictionRun);
        // 逐出扫描的时间间隔(毫秒) 如果为负数,则不运行逐出线程, 默认-1
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        // 是否在从池中取出连接前进行检验,如果检验失败,则从池中去除连接并尝试取出另一个
        jedisPoolConfig.setTestOnBorrow(testOnBorrow);
        // 在空闲时检查有效性, 默认false
        jedisPoolConfig.setTestWhileIdle(testWhileIdle);
        //表示idle object evitor两次扫描之间要sleep的毫秒数
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(30000);
        //表示idle object evitor每次扫描的最多的对象数
        jedisPoolConfig.setNumTestsPerEvictionRun(10);
        //表示一个对象至少停留在idle状态的最短时间，然后才能被idle object evitor扫描并驱逐；这一项只有在timeBetweenEvictionRunsMillis大于0时才有意义
        jedisPoolConfig.setMinEvictableIdleTimeMillis(60000);
        return jedisPoolConfig;
    }
    
    @SuppressWarnings("static-access")
	@Bean(name = "redisPool")
    public RedisPool redisPool(JedisPoolConfig jedisConfig) {
        RedisPool redisPool = new RedisPool();
        if (pool == null) {
            if (port == null) {
                port = Protocol.DEFAULT_PORT;
            }

            if (StringUtils.isBlank(password)) {
                pool = new JedisPool(jedisConfig, host, port);
            } else {
                pool = new JedisPool(jedisConfig, host, port, Protocol.DEFAULT_TIMEOUT, password, database);
            }
        }
        redisPool.setPool(pool);
        return redisPool;
    }
	
}