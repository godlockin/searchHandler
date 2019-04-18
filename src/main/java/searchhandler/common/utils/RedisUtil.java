package searchhandler.common.utils;

import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class RedisUtil {

	private static JedisPool pool;
	static {
		pool = RedisPool.getPool();
	}

	public static boolean exists(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.exists(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return false;
		}
	}

	public static void hset(String key, String field, String value) {
		try (Jedis jedis = getResource()) {
			jedis.hset(key, field, value);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	public static void hmset(String key, Map<String, String> hash) {
		try (Jedis jedis = getResource()) {
			jedis.hmset(key, hash);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	public static Long hdel(String key, String field) {
		try (Jedis jedis = getResource()) {
			return jedis.hdel(key, field);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public static String hget(String key, String field) {
		try (Jedis jedis = getResource()) {
			return jedis.hget(key, field);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}
	
	public static Map<String, String> hgetAll(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.hgetAll(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public static void set(String key, String value) {
		try (Jedis jedis = getResource()) {
			jedis.set(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	public static String get(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.get(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public static List<String> mget(String...keys) {
		try (Jedis jedis = getResource()) {
			return jedis.mget(keys);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public static Long delete(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.del(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	public List<String> mget(String key, String...fields) {
		try (Jedis jedis = getResource()) {
			return jedis.hmget(key, fields);
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	/**
	 * 设置超时失效的键值对
	 * 
	 * @param key
	 *            键
	 * @param value
	 *            值
	 * @param pSeconds
	 *            超时时间-秒
	 */

	public static String psetex(String key, String value, Long pSeconds) {
		String result = "";
		try (Jedis jedis = getResource();) {
			result = jedis.psetex(key, pSeconds * 1000l, value);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
		return result;
	}

	public Long getCounter(String key) {
		try (Jedis jedis = getResource();) {
			Long result = jedis.incrBy(key, 1L);
			if (result.equals(Long.MAX_VALUE)) {
				synchronized (result) {
					jedis.del(key);
				}
				result = jedis.incrBy(key, 1L);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	/**
	 * 向zset中指定key的member加一个值, 一致性要求不高
	 * 
	 * @param key
	 * @param value
	 *            可正可负，
	 * @param member
	 * @return
	 */
	public static Double zincrby(String key, Double value, String member) {
		try (Jedis jedis = getResource()) {
			Double result = jedis.zincrby(key, value, member);
			if (result.equals(Double.MAX_VALUE)) {
				jedis.zincrby(key, -1, member);
			} else if (result < 0) {
				jedis.zincrby(key, 0 - result, member);
			}
			return result;
		} catch (Exception e) {
			log.error(e.getMessage());
			return null;
		}
	}

	/**
	 * 对List的操作
	 * 
	 * @return 插入的索引
	 */
	public static long lpush(String key, String value) {
		try (Jedis jedis = getResource()) {
			return jedis.lpush(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			return -1l;
		}
	}

	public static String rpop(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.rpop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return "";
		}
	}

	public static String lpop(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.lpop(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return "";
		}
	}

	/**
	 * 从队尾裁剪
	 * 
	 * @param key
	 * @param start
	 *            裁剪前队尾起始索引
	 * @param end
	 *            裁剪前队头索引
	 */
	public static void ltrim(String key, int start, int end) {
		try (Jedis jedis = getResource()) {
			jedis.ltrim(key, start, end);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}

	/**
	 * 删除一个等于value的值
	 * 
	 * @param key
	 * @param value
	 */
	public static void lremOne(String key, String value) {
		try (Jedis jedis = getResource()) {
			jedis.lrem(key, 1, value);
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	/**
     * expireDelete 用1ms超时的机制来删除key
     * @param key
     */
    public static void expireDelete(String key) {
        try (Jedis jedis = getResource()) {
            jedis.pexpire(key, 1L);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }
    
    /**
     * 
     * 获取事物操作
     * */
    public static Transaction getTransaction() {
        try (Jedis jedis = getResource()) {
            return jedis.multi();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }

	private static Jedis getResource() {
		try {
			return pool.getResource();
		} catch (Exception e) {
			return getResource();
		}
	}

	/**
	 * 向名称为key的set添加
	 *
	 * @param key
	 * @param value
	 */
	public static long sadd(String key, String...value) {
		try (Jedis jedis = getResource()) {
			return jedis.sadd(key, value);
		} catch (Exception e) {
			log.error(e.getMessage());
			return -1l;
		}
	}

	public static Set<String> smembers(String key) {
		try (Jedis jedis = getResource()) {
			return jedis.smembers(key);
		} catch (Exception e) {
			log.error(e.getMessage());
			return new HashSet<>();
		}
	}
}
