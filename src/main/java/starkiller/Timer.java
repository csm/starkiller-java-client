/*
 * This class is derived from parts of the namespace
 * clojure.core.async.impl.timers, and as such this source
 * file is licensed under the Eclipse Public License, the
 * license of clojure.
 */

package starkiller;

import java.util.Map;
import java.util.concurrent.*;

public class Timer {
    private static final long TIMEOUT_RESOLUTION_MS = 10;
    private static final DelayQueue<TimeoutEntry> timeoutQueue = new DelayQueue<>();
    private static final ConcurrentSkipListMap<Long, TimeoutEntry> entryMap = new ConcurrentSkipListMap<>();
    private static Thread timeoutWorker = null;

    private static synchronized Thread ensureWorker() {
        if (timeoutWorker == null) {
            timeoutWorker = new Thread(() -> {
                while (true) {
                    try {
                        TimeoutEntry e = timeoutQueue.take();
                        entryMap.remove(e.timestamp);
                        e.future.complete(null);
                    } catch (InterruptedException interruptedException) {
                        Util.logger.warn("interrupted in timeout worker", interruptedException);
                    }
                }
            });
            timeoutWorker.setDaemon(true);
            timeoutWorker.setName("starkiller-timeout-worker");
            timeoutWorker.start();
        }
        return timeoutWorker;
    }

    static CompletableFuture<Void> timeout(long duration, TimeUnit unit) {
        ensureWorker();
        long now = System.currentTimeMillis();
        long timestamp = ((now + TimeUnit.MILLISECONDS.convert(duration, unit)) / TIMEOUT_RESOLUTION_MS) * TIMEOUT_RESOLUTION_MS;
        Util.logger.trace("timeout:{}, unit:{}, timestamp:{}, now:{}", duration, unit, timestamp, now);
        Map.Entry<Long, TimeoutEntry> e = entryMap.ceilingEntry(timestamp);
        if (e != null && e.getKey() < timestamp + TIMEOUT_RESOLUTION_MS) {
            return e.getValue().future;
        } else {
            CompletableFuture<Void> future = new CompletableFuture<>();
            TimeoutEntry entry = new TimeoutEntry(timestamp, future);
            entryMap.put(timestamp, entry);
            timeoutQueue.put(entry);
            return future;
        }
    }

    private static class TimeoutEntry implements Delayed {
        private final long timestamp;
        final CompletableFuture<Void> future;

        public TimeoutEntry(long timestamp, CompletableFuture<Void> future) {
            this.timestamp = timestamp;
            this.future = future;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(timestamp - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(timestamp, ((TimeoutEntry) o).timestamp);
        }
    }
}
