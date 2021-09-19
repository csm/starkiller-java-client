package starkiller;

import com.fasterxml.jackson.databind.*;
import org.cliffc.high_scale_lib.NonBlockingHashMapLong;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

@SuppressWarnings("unchecked")
public class RemoteJunction<K, V> implements Junction<K, V>, Closeable {
    private static final Logger logger = LoggerFactory.getLogger(RemoteJunction.class);

    @Override
    public void close() throws IOException {
        stop();
        socket.close();
    }

    public static class Request<K, V> {
        public long messageId;
    }

    public static class RecvRequest<K, V> extends Request<K, V> {
        public K id;
        public Long timeout;
    }

    public static class SendRequest<K, V> extends Request<K, V> {
        public Long timeout;
        public K id;
        public V value;
    }

    public static class TokenRequest<K, V> extends Request<K, V> {
    }

    public static class Response<V> {
        public long messageId;
    }

    public static class RecvResponse<V> extends Response<V> {
        public V value;

        public RecvResponse(long messageId, V value) {
            this.messageId = messageId;
            this.value = value;
        }
    }

    public static class SendResponse<V> extends Response<V> {
        public boolean success;

        public SendResponse(long messageId, boolean success) {
            this.messageId = messageId;
            this.success = success;
        }
    }

    public static class TokenResponse<V> extends Response<V> {
        public List<Long> tokens;

        public TokenResponse(long messageId, List<Long> tokens) {
            this.messageId = messageId;
            this.tokens = tokens;
        }
    }

    public static class TimeoutResponse<V> extends Response<V> {
        public TimeoutResponse(long messageId) {
            this.messageId = messageId;
        }
    }

    private final ByteBuffer lengthBuffer = ByteBuffer.allocate(2);
    private final AtomicLong messageIdGen = new AtomicLong();
    private final AtomicBoolean running = new AtomicBoolean();
    private final NonBlockingHashMapLong<CompletableFuture> methodCalls = new NonBlockingHashMapLong<>();
    private final AsynchronousSocketChannel socket;
    private final MsgPack<K, V> msgpack;
    private final Util.AsyncLock writeLock = new Util.AsyncLock();

    public RemoteJunction(AsynchronousSocketChannel socket, Consumer<ObjectMapper> objectMapperConfigurator) {
        this.socket = socket;
        ObjectMapper objectMapper = new ObjectMapper(new MessagePackFactory());
        objectMapperConfigurator.accept(objectMapper);
        msgpack = new MsgPack<K, V>(objectMapper);
    }

    public RemoteJunction(AsynchronousSocketChannel socket) {
        this(socket, (om) -> {});
    }

    private CompletableFuture<Void> runningFuture = CompletableFuture.failedFuture(new IllegalStateException("not running"));

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

    CompletableFuture<Void> readLoop() {
        logger.trace("readLoop begin");
        return Util.readMessage(socket).thenCompose(buffer -> {
            try {
                Response<V> resp = msgpack.unpackResponse(buffer);
                logger.trace("read response {}", resp);
                CompletableFuture future = methodCalls.remove(resp.messageId);
                if (future != null) {
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
                }
                if (running.get()) {
                    return readLoop();
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } catch (IOException e) {
                return CompletableFuture.failedFuture(e);
            }
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
        CompletableFuture<Void> writeFuture =  writeLock.lock().thenCompose(l -> Util.writeFully(socket, buffer));
        writeFuture.whenComplete((res, exc) -> writeLock.unlock());
        return writeFuture.thenCompose(x -> future);
    }
}
