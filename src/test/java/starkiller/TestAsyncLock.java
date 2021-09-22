package starkiller;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class TestAsyncLock {
    @Test
    public void testLock() throws InterruptedException, ExecutionException, TimeoutException {
        Util.AsyncLock lock = new Util.AsyncLock();
        Boolean result = lock.lock().thenApply(l -> {
            System.out.println("locked!");
            l.unlock();
            return true;
        }).get(10, TimeUnit.SECONDS);
        assertEquals(Boolean.TRUE, result);
        result = lock.lock().thenApply(l -> {
            l.unlock();
            return true;
        }).get(10, TimeUnit.SECONDS);
        assertEquals(Boolean.TRUE, result);
    }

    @Test
    public void testLocks() throws ExecutionException, InterruptedException {
        Random r = new Random();
        AtomicInteger value = new AtomicInteger();
        Util.AsyncLock lock = new Util.AsyncLock();
        CompletableFuture[] futures = new CompletableFuture[100];
        for (int i = 0; i < 100; i++) {
            futures[i] = lock.lock().thenComposeAsync(l -> {
                System.out.printf("got lock %s%n", l);
                int v = value.get();
                return Timer.timeout(r.nextInt(100), TimeUnit.MILLISECONDS).thenApply(x -> {
                    value.set(v + 1);
                    l.unlock();
                    return null;
                });
            });
        }
        CompletableFuture.allOf(futures).get();
        assertEquals(100, value.get());
    }
}
