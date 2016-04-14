package com.redislabs.provider.redis

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import com.redislabs.provider.redis.rdd._

import redis.clients.jedis.Pipeline

/**
  * RedisContext extends sparkContext's functionality with redis functions
  *
  * @param sc a spark context
  */
class RedisContext(@transient val sc: SparkContext) extends Serializable {

  import com.redislabs.provider.redis.RedisContext._

  /**
    * @param keyPattern a key pattern to match, or a single key
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeyPattern(keyPattern: String = "*",
                          partitionNum: Int = 3)
                         (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
  RedisKeysRDD = {

    new RedisKeysRDD(sc, redisConfig, keyPattern, partitionNum, null);
  }

  /**
    * @param keys an array of keys
    * @param partitionNum number of partitions
    * @return RedisKeysRDD of simple Keys stored in redis server
    */
  def fromRedisKeys(keys: Array[String],
                    partitionNum: Int = 3)
                   (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))):
  RedisKeysRDD = {

    new RedisKeysRDD(sc, redisConfig, "", partitionNum, keys);
  }

  /**
    * @param kvs Pair RDD of K/V
    * @param ttl time to live
    */
  def toRedisKV(kvs: RDD[(String, String)], ttl: Int = 0)
     (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setKVs(partition, ttl, redisConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param hashName target hash's name which hold all the kvs
    * @param ttl time to live
    */
  def toRedisHASH(kvs: RDD[(String, String)], hashName: String, ttl: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setHash(hashName, partition, ttl, redisConfig))
  }

  /**
    * @param kvs      Pair RDD of K/V
    * @param zsetName target zset's name which hold all the kvs
    * @param ttl time to live
    */
  def toRedisZSET(kvs: RDD[(String, String)], zsetName: String, ttl: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreachPartition(partition => setZset(zsetName, partition, ttl, redisConfig))
  }
  
  /**
    * @param kvs      Pair RDD of K/V
    * @param ttl time to live
    */
  def toRedisMultikeyZSET(kvs: RDD[(String, Iterator[(String,String)])], ttl: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    kvs.foreach(partition => setMultikeyZset(partition, ttl, redisConfig))
  }

  /**
   *
   * @param kvs RDD of Key Value pairs
   * @param ttl time to live for key
   * @param redisConfig redis config
   * @return
   */
  def toRedisMultikeySet(kvs: RDD[(String, Iterator[String])], ttl: Int = 0)
                        (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
    kvs.foreach(partition => setMultikeySet(partition, ttl, redisConfig))
  }


  /**
    * @param vs      RDD of values
    * @param setName target set's name which hold all the vs
    * @param ttl time to live
    */
  def toRedisSET(vs: RDD[String], setName: String, ttl: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setSet(setName, partition, ttl, redisConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    * @param ttl time to live
    */
  def toRedisLIST(vs: RDD[String], listName: String, ttl: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setList(listName, partition, ttl, redisConfig))
  }

  /**
    * @param vs       RDD of values
    * @param listName target list's name which hold all the vs
    * @param listSize target list's size
    *                 save all the vs to listName(list type) in redis-server
    */
  def toRedisFixedLIST(vs: RDD[String],
                       listName: String,
                       listSize: Int = 0)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {

    vs.foreachPartition(partition => setFixedList(listName, listSize, partition, redisConfig))
  }
  
  /**
    * @param vs       RDD of values
    *                 save all the vs to listName(list type) in redis-server
    */
  def toRedisIncrBy(vs: RDD[String], value: Int)
    (implicit redisConfig: RedisConfig = new RedisConfig(new RedisEndpoint(sc.getConf))) {
    vs.foreach(unit => setIncrBy(unit, value, redisConfig))
  }

}


object RedisContext extends Serializable {
  /**
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to the target host
    * @param ttl time to live
    */
  def setKVs(arr: Iterator[(String, String)], ttl: Int, redisConfig: RedisConfig) {

    arr.map(kv => (redisConfig.getHost(kv._1), kv)).toArray.groupBy(_._1).
      mapValues(a => a.map(p => p._2)).foreach {
      x => {
        val conn = x._1.endpoint.connect()
        val pipeline = x._1.endpoint.connect.pipelined
        if (ttl <= 0) {
          x._2.foreach(x => pipeline.set(x._1, x._2))
        }
        else {
          x._2.foreach(x => pipeline.setex(x._1, ttl, x._2))
        }
        pipeline.sync()
        conn.close()
      }
    }
  }


  /**
    * @param hashName redis hash name
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to hashName(list type) to the target host
    * @param ttl time to live
    */
  def setHash(hashName: String, arr: Iterator[(String, String)],
              ttl: Int, redisConfig: RedisConfig) {

    val conn = redisConfig.connectionForKey(hashName)
    val pipeline = conn.pipelined
    arr.foreach(x => pipeline.hset(hashName, x._1, x._2))
    if (ttl > 0) pipeline.expire(hashName, ttl)
    pipeline.sync()
    conn.close()
  }

  /**
    * @param zsetName redis sorted set name
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to zsetName(zset type) to the target host
    * @param ttl time to live
    */
  def setZset(zsetName: String, arr: Iterator[(String, String)],
              ttl: Int, redisConfig: RedisConfig) {
    val jedis = redisConfig.connectionForKey(zsetName)
    val pipeline = jedis.pipelined
    arr.foreach(x => pipeline.zadd(zsetName, x._2.toDouble, x._1))
    if (ttl > 0) pipeline.expire(zsetName, ttl)
    pipeline.sync()
    jedis.close()
  }
  
  /**
    * @param arr k/vs which should be saved in the target host
    *            save all the k/vs to zsetName(zset type) to the target host
    * @param ttl time to live
    */
  def setMultikeyZset(arr: (String,Iterator[(String, String)]),
                      ttl: Int, redisConfig: RedisConfig) {
    val jedis = redisConfig.connectionForKey(arr._1)
    val pipeline = jedis.pipelined
    arr._2.foreach(y => pipeline.zadd(arr._1, y._2.toDouble, y._1))
    if (ttl > 0) pipeline.expire(arr._1, ttl)
    pipeline.sync()
    jedis.close()
  }

  /**
   *
   * @param arr Array of RDD data
   * @param ttl time to live for key
   * @param redisConfig redis config
   */
  def setMultikeySet(arr: (String, Iterator[String]),
                     ttl: Int, redisConfig: RedisConfig) {
    val jedis = redisConfig.connectionForKey(arr._1)
    val pipeline = jedis.pipelined
    arr._2.foreach(y => pipeline.sadd(arr._1, y))
    if (ttl > 0) pipeline.expire(arr._1, ttl)
    pipeline.sync()
    jedis.close()
  }

  /**
    * @param setName set name
    * @param arr values which should be saved in the target host
    *            save all the values to setName(set type) to the target host
    * @param ttl time to live
    */
  def setSet(setName: String, arr: Iterator[String],
             ttl: Int, redisConfig: RedisConfig) {

    val jedis = redisConfig.connectionForKey(setName)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.sadd(setName, _))
    if (ttl > 0) pipeline.expire(setName, ttl)
    pipeline.sync()
    jedis.close()
  }

  /**
    * @param listName list name
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    * @param ttl time to live
    */
  def setList(listName: String, arr: Iterator[String],
              ttl: Int, redisConfig: RedisConfig) {

    val jedis = redisConfig.connectionForKey(listName)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.rpush(listName, _))
    if (ttl > 0) pipeline.expire(listName, ttl)
    pipeline.sync()
    jedis.close()
  }

  /**
    * @param key redis key
    * @param listSize list size
    * @param arr values which should be saved in the target host
    *            save all the values to listName(list type) to the target host
    */
  def setFixedList(key: String,
                   listSize: Int,
                   arr: Iterator[String],
                   redisConfig: RedisConfig) {

    val jedis = redisConfig.connectionForKey(key)
    val pipeline = jedis.pipelined
    arr.foreach(pipeline.lpush(key, _))
    if (listSize > 0) {
      pipeline.ltrim(key, 0, listSize - 1)
    }
    pipeline.sync()
    jedis.close()
  }
  
  def setIncrBy(key: String,
                value: Int=1,
                redisConfig: RedisConfig) {
    val jedis = redisConfig.connectionForKey(key)
    val pipeline = jedis.pipelined
    pipeline.incrBy(key, value)
    pipeline.sync()
    jedis.close()
  }


  /**
   * @param key redis key
   * @param operation is the Redis Function in the Pipeline
   * @param config is the Map[String, String]
   */
  def setMultiFunctions(key: String,
                        vab1: Object,
                        vab2: Object,
                        pipeline: Pipeline,
                        operation: String,
                        config: Map[String,String]){

    if (operation.equalsIgnoreCase("zadd")) {
      pipeline.zadd(key, vab2.toString.toDouble, vab1.toString)
    } else if(operation.equalsIgnoreCase("incr")) {
      pipeline.incr(key)
    } else if(operation.equalsIgnoreCase("incrBy")) {
      pipeline.incrBy(key, config.get("value").head.toLong)
    }
  }
}

trait RedisFunctions {
  implicit def toRedisContext(sc: SparkContext): RedisContext = new RedisContext(sc)
}

