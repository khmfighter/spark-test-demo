package com.extr

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}


/**
 * Created by spark on 11/19/15.
 */
object WordCount {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark Pi").setMaster("local[4]")//.setMaster("spark://master99:7077")
    val spark = new SparkContext(conf)


    val count = spark.parallelize(1 until 50).map { i => i + 1 }.reduce(_ + _)

    println("Pi is roughly " + count)

    val nodes = new util.HashSet[HostAndPort]
    nodes.add(new HostAndPort("54.169.249.133", 6400))
    val jpool = new JedisPoolConfig()
    jpool.setMaxTotal(6000)
    jpool.setMaxIdle(500)
    jpool.setTimeBetweenEvictionRunsMillis(30000)
    jpool.setMinEvictableIdleTimeMillis(30000)
    jpool.setTestOnBorrow(true)
    val jedisCluster = new JedisCluster(nodes,jpool)


    jedisCluster.set("redis", "myredis")
    println(jedisCluster.get("redis"))

    spark.stop()
  }


}


//object RedisFactoryHolder {
//
//
//  def jedisCluster: JedisCluster = null
//
//  def apply {
//    if (jedisCluster == null) {
//      val nodes = new mutable.HashSet [ HostAndPort ]()
//      nodes.add(new HostAndPort("54.169.249.133", 6400))
//      val jpool = new JedisPoolConfig()
//      jpool.setMaxTotal(6000)
//      jpool.setMaxIdle(500)
//      jpool.setTimeBetweenEvictionRunsMillis(30000)
//      jpool.setMinEvictableIdleTimeMillis(30000)
//      jpool.setTestOnBorrow(true);
//      jedisCluster = new JedisCluster(nodes,jpool)
//    }
//  }
//
//}