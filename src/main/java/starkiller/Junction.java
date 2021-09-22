package starkiller;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A junction. Allows communication on ID's (which can be anything)
 * and don't need to be created ahead of time.
 *
 * @param <K> The identifier type. This must be encodable with MsgPack.
 * @param <V> The value type. This must be encodable with MsgPack.
 */
public interface Junction<K, V> {
    /**
     * Send a value to an ID.
     *
     * @param id The ID to send to.
     * @param value The value to send.
     * @param timeout The timeout value.
     * @param timeoutUnit The timeout unit
     * @return A future that will yield true on successful send, or an exception
     *         {@link TimeoutException} on timeout, or another exception on error.
     */
    CompletableFuture<Boolean> send(K id, V value, long timeout, TimeUnit timeoutUnit);

    /**
     * Receive a value from an ID.
     *
     * @param id The ID to receive on.
     * @param timeout The timeout value.
     * @param timeoutUnit The timeout unit.
     * @return A future that will yield true on successful send, or an exception
     *         {@link TimeoutException} on timeout, or another exception on error.
     */
    CompletableFuture<V> recv(K id, long timeout, TimeUnit timeoutUnit);
}
