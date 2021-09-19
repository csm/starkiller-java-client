package starkiller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A simple byte-array-oriented junction.
 *
 * Richer, typed junctions may be built atop this.
 */
public interface Junction<K, V> {
    CompletableFuture<Boolean> send(K id, V value, long timeout, TimeUnit timeoutUnit);
    CompletableFuture<V> recv(K id, long timout, TimeUnit timeoutUnit);
}
