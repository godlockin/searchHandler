package searchhandler.common.utils;

import redis.clients.jedis.JedisPool;

/**
 * redis的连接池
 * 
 * @author lijie
 *
 */
public class RedisPool {
	private static JedisPool pool = null;
	
	static public void init() {

	}

	static public void close() {
		if (pool != null) {
			pool.close();
		}
	}
	
	static public JedisPool setPool(JedisPool jedispool) {
		pool = jedispool;
		return pool;
	}

	static public JedisPool getPool() {
		return pool;
	}
}
