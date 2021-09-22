package starkiller;

import io.lacuna.bifurcan.IList;
import io.lacuna.bifurcan.LinearList;
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
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class Util {
    public static final Logger logger = LoggerFactory.getLogger(Util.class);

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
                    message.flip();
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

    static class Atom<T> {
        final AtomicReference<T> reference;

        public Atom(T initial) {
            reference = new AtomicReference<>(initial);
        }

        Atom() {
            this(null);
        }

        void swap(Function<T, T> applier) {
            while (true) {
                T current = reference.get();
                T next = applier.apply(current);
                if (reference.compareAndSet(current, next)) {
                    break;
                }
            }
        }

        T deref() {
            return reference.get();
        }
    }

    static class AsyncLock {
        private final String name;
        private final AtomicInteger idgen = new AtomicInteger();
        private final AtomicReference<String> locked = new AtomicReference<>();
        private final Atom<IList<Waiter>> waiters = new Atom<>(LinearList.of());

        public AsyncLock(String name) {
            this.name = name;
        }

        public AsyncLock() {
            this(null);
        }

        class Locked {
            final String id = String.format("%s-%d", name != null ? name : "lock", idgen.incrementAndGet());

            void unlock() {
                if (locked.compareAndSet(id, null)) {
                    AtomicReference<Waiter> selected = new AtomicReference<>();
                    waiters.swap(l -> {
                        if (l.size() > 0) {
                            Waiter head = l.first();
                            selected.set(head);
                            return l.removeFirst();
                        } else {
                            return l;
                        }
                    });
                    if (selected.get() != null) {
                        if (locked.compareAndSet(null, selected.get().locked.id)) {
                            selected.get().complete();
                        } else {
                            waiters.swap(l -> l.addFirst(selected.get()));
                        }
                    }
                }
            }
        }

        class Waiter {
            final Locked locked;
            final CompletableFuture<Locked> future;

            public Waiter(Locked locked, CompletableFuture<Locked> future) {
                this.locked = locked;
                this.future = future;
            }

            void complete() {
                future.complete(locked);
            }
        }

        public CompletableFuture<Locked> lock() {
            logger.trace("locking {} {}", this, this.name);
            Locked l = new Locked();
            if (locked.compareAndSet(null, l.id)) {
                logger.trace("locked {}", this);
                return CompletableFuture.completedFuture(l);
            } else {
                logger.trace("waiting for lock {}", this);
                Waiter waiter = new Waiter(l, new CompletableFuture<>());
                logger.trace("waiting for lock {} with {}", this, waiter);
                waiters.swap(w -> w.addLast(waiter));
                waiter.future.whenComplete((res, exc) -> logger.trace("waiter {} completed with res:{}, exc:{}", waiter, res, exc));
                return waiter.future;
            }
        }
    }

}
