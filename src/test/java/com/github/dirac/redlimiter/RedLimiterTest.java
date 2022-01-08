package com.github.dirac.redlimiter;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

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

        List<Future<Object>> objects = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            pool.execute(() -> {
                double acquire = limiter.acquire(1);
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });

            pool.submit(() -> {
                Double acquire = limiter.acquire(1);
                System.out.println(index + " \t" + acquire + " \t" + new Date());
                try {
                    Thread.sleep(acquire.intValue());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
        }
        Thread.sleep(12 * 1000L);
    }

    @org.junit.Test
    public void tryAcquire() throws Exception {
        List<Future<Object>> objects = Lists.newArrayList();
        for (int i = 0; i < 10; i++) {
            final int index = i;
            pool.execute(() -> {
                boolean acquire = limiter.tryAcquire();
                System.out.println(index + " \t" + acquire + " \t" + new Date());
            });
            Future<Object> submit = pool.submit((Callable<Object>) () -> {
                while (1 == 1) {
                    Double acquire = limiter.acquire();
                    if (acquire.equals(Double.valueOf(0))) {
                        System.out.println(index + " \t" + acquire + " \t" + "执行！");
                        break;
                    }
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        System.out.println(e.getMessage());
                    }
                    System.out.println(index + " \t" + acquire + " \t" + new Date());
                }
                return true;
            });
            objects.add(submit);

        }
        for (Future<Object> object : objects) {
            System.out.println(object.get());
        }
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