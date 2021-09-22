package starkiller;

import org.junit.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestTimeout {
    @Test
    public void testTimeout() throws ExecutionException, InterruptedException {
        long start = System.currentTimeMillis();
        long end = Timer.timeout(1, TimeUnit.SECONDS).thenApply(x -> System.currentTimeMillis()).get();
        System.out.printf("start:%d end:%d diff:%d", start, end, end - start);
        assertTrue((end - start) >= 1000);
    }
}
