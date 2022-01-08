package com.github.dirac.redlimiter;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Ignore
public class RedLimiterTest {

    private static RedLimiter limiter;

    @BeforeClass
    public static void init() {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(200);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost");
        limiter = RedLimiter.create("1000", 2, jedisPool,true);
    }

    @After
    public void tearDown() throws Exception {
        Thread.sleep(2000L);
    }

    private ExecutorService pool = Executors.newFixedThreadPool(500);

    @org.junit.Test
    public void acquire() throws Exception {
        for (int i = 0; i < 10; i++) {
            final int index = i;
            pool.execute(() -> {
                double acquire = limiter.acquire(1);
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
        }
        Thread.sleep(12 * 1000L);
    }

    @org.junit.Test
    public void tryAcquire() throws Exception {
        for (int i = 0; i < 10; i++) {
            final int index = i;
            pool.execute(() -> {
                boolean acquire = limiter.tryAcquire();
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
        }
        Thread.sleep(5 * 1000L);
    }

    @org.junit.Test
    public void tryAcquireTimeout() throws Exception {
        for (int i = 0; i < 10; i++) {
            final int index = i;
            pool.execute(() -> {
                boolean acquire = limiter.tryAcquire(1000L, TimeUnit.MILLISECONDS);
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
        }
        Thread.sleep(10 * 1000L);
    }

    @Test
    public void batchAcquireLazy() throws Exception {
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxTotal(50);
        JedisPool jedisPool = new JedisPool(jedisPoolConfig, "localhost");
        RedLimiter redLimiter = RedLimiter.create("500", 1000, jedisPool, true);
        redLimiter.setBatchSize(50);
        for (int i = 0; i < 1000; i++) {
            final int index = i;
            pool.execute(() -> {
                double acquire = redLimiter.acquireLazy(10);
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
        }
        Thread.sleep(10 * 1000L);
    }
}