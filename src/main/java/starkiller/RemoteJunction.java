package starkiller;

import com.fasterxml.jackson.databind.*;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A remote junction.
 *
 * @param <K> The ID type.
 * @param <V> The value type.
 */
@SuppressWarnings("unchecked")
public class RemoteJunction<K, V> implements Junction<K, V>, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJunction.class);

    @Override
    public void close() throws IOException {
        stop();
        AsynchronousSocketChannel s = socket.get();
        if (s != null) {
            s.close();
        }
        methodCalls.forEach((id, action) -> {
            action.completeExceptionally(new ClosedChannelException());
        });
    }

    static class Request<K, V> {
        public long messageId;
    }

    static class RecvRequest<K, V> extends Request<K, V> {
        public K id;
        public Long timeout;
    }

    static class SendRequest<K, V> extends Request<K, V> {
        public Long timeout;
        public K id;
        public V value;
    }

    static class TokenRequest<K, V> extends Request<K, V> {
    }

    static class Response<V> {
        public long messageId;
    }

    static class RecvResponse<V> extends Response<V> {
        public V value;

        public RecvResponse(long messageId, V value) {
            this.messageId = messageId;
            this.value = value;
        }
    }

    static class SendResponse<V> extends Response<V> {
        public boolean success;

        public SendResponse(long messageId, boolean success) {
            this.messageId = messageId;
            this.success = success;
        }
    }

    static class TokenResponse<V> extends Response<V> {
        public List<Long> tokens;

        public TokenResponse(long messageId, List<Long> tokens) {
            this.messageId = messageId;
            this.tokens = tokens;
        }
    }

    static class TimeoutResponse<V> extends Response<V> {
        public TimeoutResponse(long messageId) {
            this.messageId = messageId;
        }
    }

    private final InetSocketAddress address;
    private final AtomicLong messageIdGen = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();
    private final NonBlockingHashMapLong<CompletableFuture> methodCalls = new NonBlockingHashMapLong<>();
    private final AtomicReference<AsynchronousSocketChannel> socket = new AtomicReference<>();
    private final MsgPack<K, V> msgpack;
    private final Util.AsyncLock connectLock = new Util.AsyncLock("connect");
    private final Util.AsyncLock writeLock = new Util.AsyncLock("write");

    /**
     * Create a new remote junction.
     *
     * You must call {@link #start()} to begin the read loop.
     *
     * @param address The address to connect to.
     * @param valueClass The class your values will be.
     * @param objectMapperConfigurator A consumer for performing additional
     *                                 configuration on the {@link ObjectMapper}
     *                                 to be created.
     */
    public RemoteJunction(InetSocketAddress address, Class<? extends V> valueClass, Consumer<ObjectMapper> objectMapperConfigurator) {
        this.address = address;
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        objectMapperConfigurator.accept(objectMapper);
        msgpack = new MsgPack<>(objectMapper, valueClass);
    }

    /**
     * Create a new remote junction.
     *
     * You must call {@link #start()} to begin the read loop.
     *
     * @param address The address to connect to.
     * @param valueClass The class your values will be.
     */
    public RemoteJunction(InetSocketAddress address, Class<? extends V> valueClass) {
        this(address, valueClass, (om) -> {});
    }

    private CompletableFuture<Void> runningFuture = CompletableFuture.failedFuture(new IllegalStateException("not running"));

    /**
     * Start the asynchronous read loop.
     *
     * @return A future that will yield null when the read loop completes
     *         because {@link #stop()} was called, or with an exception
     *         on failures.
     */
    public CompletableFuture<Void> start() {
        if (running.compareAndSet(false, true)) {
            return runningFuture = readLoop();
        }
        return runningFuture;
    }

    public void stop() {
        running.set(false);
    }

    public boolean isRunning() {
        return !runningFuture.isDone();
    }

    CompletableFuture<AsynchronousSocketChannel> ensureSocket() {
        AsynchronousSocketChannel s = socket.get();
        if (s != null && s.isOpen()) {
            return CompletableFuture.completedFuture(s);
        }
        return connectLock.lock().thenCompose(l -> {
            if (socket.get() != null && socket.get().isOpen()) {
                return CompletableFuture.completedFuture(socket.get());
            } else {
                try {
                    AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                    CompletableFuture<AsynchronousSocketChannel> future = new CompletableFuture<>();
                    socket.connect(this.address, null, new CompletionHandler<>() {
                        @Override
                        public void completed(Void result, Object attachment) {
                            RemoteJunction.this.socket.set(socket);
                            future.complete(socket);
                            l.unlock();
                        }

                        @Override
                        public void failed(Throwable exc, Object attachment) {
                            future.completeExceptionally(exc);
                            l.unlock();
                        }
                    });
                    return future;
                } catch (IOException e) {
                    return CompletableFuture.failedFuture(e);
                }
            }
        });
    }

    CompletableFuture<Void> readLoop() {
        logger.trace("readLoop begin");
        return ensureSocket().thenCompose(socket -> {
            CompletableFuture<ByteBuffer> readFuture = Util.readMessage(socket);
            CompletableFuture<Boolean> dispatchFuture = readFuture.thenApply(buffer -> {
                try {
                    logger.trace("read message buffer: {}", Util.hex(buffer));
                    Response<V> resp = msgpack.unpackResponse(buffer);
                    logger.trace("read response {}", resp);
                    CompletableFuture future = methodCalls.remove(resp.messageId);
                    if (future != null) {
                        logger.trace("completing call {} {} with {}", resp.messageId, future, resp);
                        if (resp instanceof RemoteJunction.RecvResponse) {
                            future.complete(((RecvResponse<V>) resp).value);
                        } else if (resp instanceof RemoteJunction.SendResponse) {
                            future.complete(Boolean.TRUE);
                        } else if (resp instanceof RemoteJunction.TokenResponse) {
                            future.complete(((TokenResponse<V>) resp).tokens);
                        } else if (resp instanceof RemoteJunction.TimeoutResponse) {
                            future.completeExceptionally(new TimeoutException());
                        } else {
                            future.completeExceptionally(new IllegalStateException("should never have decoded a " + resp));
                        }
                    } else {
                        logger.warn("no method call resolved for {}, response {}", resp.messageId, resp);
                    }
                    return running.get();
                } catch (IOException e) {
                    logger.warn("exception parsing message", e);
                    return false;
                }
            }).exceptionally(exc -> {
                logger.warn("exception reading message", exc);
                if (exc instanceof ClosedChannelException || exc.getCause() instanceof ClosedChannelException) {
                    this.socket.set(null);
                    return true;
                } else {
                    try {
                        close();
                    } catch (IOException e) {
                        // pass
                    }
                    return false;
                }
            });
            return dispatchFuture.thenComposeAsync((Function<Boolean, CompletableFuture<Void>>) again -> {
                CompletableFuture<Void> nextFuture;
                if (again) {
                    nextFuture = readLoop();
                } else {
                    nextFuture = CompletableFuture.completedFuture(null);
                }
                return nextFuture;
            });
        });
    }

    @Override
    public CompletableFuture<Boolean> send(K id, V value, long timeout, TimeUnit timeoutUnit) {
        SendRequest<K, V> request = new SendRequest<>();
        request.id = id;
        request.value = value;
        request.timeout = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
        request.messageId = messageIdGen.incrementAndGet();
        return sendRequest(request);
    }

    @Override
    public CompletableFuture<V> recv(K id, long timeout, TimeUnit timeoutUnit) {
        RecvRequest<K, V> request = new RecvRequest<>();
        request.id = id;
        request.timeout = TimeUnit.MILLISECONDS.convert(timeout, timeoutUnit);
        request.messageId = messageIdGen.incrementAndGet();
        return sendRequest(request);
    }

    public CompletableFuture<List<Long>> tokens() {
        TokenRequest<K, V> request = new TokenRequest<>();
        request.messageId = messageIdGen.incrementAndGet();
        return sendRequest(request);
    }

    <T> CompletableFuture<T> sendRequest(Request<K, V> r) {
        CompletableFuture<T> future = new CompletableFuture<>();
        byte[] encoded;
        try {
            encoded = msgpack.packRequest(r);
        } catch (IOException e) {
            future.completeExceptionally(e);
            return future;
        }
        ByteBuffer buffer = ByteBuffer.allocate(encoded.length + 2);
        buffer.putShort((short) encoded.length);
        buffer.put(encoded);
        buffer.flip();
        logger.trace("encoded message {}", Util.hex(buffer));
        methodCalls.put(r.messageId, future);
        AtomicReference<Util.AsyncLock.Locked> locked = new AtomicReference<>();
        CompletableFuture<Void> writeFuture = ensureSocket().thenCompose(socket -> writeLock.lock().thenCompose(l -> {
            locked.set(l);
            return Util.writeFully(socket, buffer);
        }));
        writeFuture.whenComplete((res, exc) -> {
            if (locked.get() != null) {
                locked.get().unlock();
            }
        });
        return writeFuture.thenCompose(x -> future);
    }
}
