package starkiller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

public class Util {
    public static final Logger logger = LoggerFactory.getLogger(Util.class);

    public static DataInput wrap(ByteBuffer buffer) {
        return new DataInput() {
            @Override
            public void readFully(byte[] b) throws IOException {
                readFully(b, 0, b.length);
            }

            @Override
            public void readFully(byte[] b, int off, int len) throws IOException {
                try {
                    buffer.get(b, off, len);
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public int skipBytes(int n) {
                n = Math.min(n, buffer.remaining());
                buffer.position(buffer.position() + n);
                return n;
            }

            @Override
            public boolean readBoolean() throws IOException {
                return readByte() == 0;
            }

            @Override
            public byte readByte() throws IOException {
                try {
                    return buffer.get();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public int readUnsignedByte() throws IOException {
                return readByte() & 0xFF;
            }

            @Override
            public short readShort() throws IOException {
                try {
                    return buffer.getShort();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public int readUnsignedShort() throws IOException {
                return readShort() & 0xFF;
            }

            @Override
            public char readChar() throws IOException {
                return (char) readUnsignedShort();
            }

            @Override
            public int readInt() throws IOException {
                try {
                    return buffer.getInt();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public long readLong() throws IOException {
                try {
                    return buffer.getLong();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public float readFloat() throws IOException {
                try {
                    return buffer.getFloat();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public double readDouble() throws IOException {
                try {
                    return buffer.getDouble();
                } catch (BufferUnderflowException bue) {
                    throw new EOFException();
                }
            }

            @Override
            public String readLine() throws IOException {
                StringBuilder str = new StringBuilder();
                int b;
                while ((b = readUnsignedByte()) != '\n') {
                    str.append((char) b);
                }
                return str.toString();
            }

            @Override
            public String readUTF() throws IOException {
                int len = readUnsignedShort();
                byte[] b = new byte[len];
                readFully(b);
                return new String(b, StandardCharsets.UTF_8);
            }
        };
    }

    public static Object hex(ByteBuffer buffer) {
        return new Object() {
            @Override
            public String toString() {
                StringBuilder buf = new StringBuilder();
                for (int i = buffer.position(); i < buffer.limit(); i++) {
                    buf.append(String.format("%02x", buffer.get(i) & 0xFF));
                }
                return buf.toString();
            }
        };
    }

    public static CompletableFuture<Integer> readFully(AsynchronousSocketChannel socket, ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            logger.trace("readFully remaining {}", buffer.remaining());
            CompletableFuture<Integer> future = new CompletableFuture<>();
            socket.read(buffer, null, new CompletionHandler<>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    logger.trace("readFully read {} remaining {}", result, buffer.remaining());
                    future.complete(result);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    logger.warn("readFully failed", exc);
                    future.completeExceptionally(exc);
                }
            });
            return future.thenCompose(r -> readFully(socket, buffer).thenApply(r2 -> r + r2));
        } else {
            return CompletableFuture.completedFuture(0);
        }
    }

    public static CompletableFuture<ByteBuffer> readMessage(AsynchronousSocketChannel socket) {
        ByteBuffer lengthBuffer = ByteBuffer.allocate(2);
        return readFully(socket, lengthBuffer).thenCompose(r -> {
            lengthBuffer.flip();
            int length = lengthBuffer.getShort() & 0xFFFF;
            logger.trace("read message length {}", length);
            if (length <= 0x3FFF) {
                ByteBuffer message = ByteBuffer.allocate(length);
                return readFully(socket, message).thenApply(rr -> {
                    logger.trace("read message {}", hex(message));
                    return message;
                });
            } else {
                throw new IllegalStateException("length too big: " + length);
            }
        });
    }

    static CompletableFuture<Void> writeFully(AsynchronousSocketChannel socket, ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            socket.write(buffer, null, new CompletionHandler<>() {
                @Override
                public void completed(Integer result, Object attachment) {
                    logger.trace("wrote {}, {} remaining", result, buffer.remaining());
                    future.complete(null);
                }

                @Override
                public void failed(Throwable exc, Object attachment) {
                    future.completeExceptionally(exc);
                }
            });
            return future.thenCompose(x -> writeFully(socket, buffer));
        } else {
            return CompletableFuture.completedFuture(null);
        }
    }

    static class AsyncLock {
        private final AtomicBoolean locked = new AtomicBoolean(false);
        private final LinkedList<CompletableFuture<Void>> waiters = new LinkedList<>();

        public synchronized CompletableFuture<Void> lock() {
            if (locked.compareAndSet(false, true)) {
                return CompletableFuture.completedFuture(null);
            } else {
                CompletableFuture<Void> waiter = new CompletableFuture<>();
                waiters.add(waiter);
                return waiter;
            }
        }

        public synchronized void unlock() {
            if (waiters.isEmpty()) {
                locked.set(false);
            } else {
                CompletableFuture<Void> waiter = waiters.removeFirst();
                waiter.complete(null);
            }
        }
    }
}
