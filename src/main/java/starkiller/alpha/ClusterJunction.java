package starkiller.alpha;

import starkiller.Junction;
import starkiller.RemoteJunction;

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ClusterJunction<K, V> implements Junction<K, V> {
    private final Discovery discovery;
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicReference<List<ClusterEntry>> entries = new AtomicReference<>(Collections.emptyList());
    private final AtomicReference<CompletableFuture<Object>> changedFuture = new AtomicReference<>(new CompletableFuture<>());

    class ClusterEntry implements Comparable<ClusterEntry> {
        public final long token;
        public final RemoteJunction<K, V> junction;
        public final String id;

        ClusterEntry(long token, RemoteJunction<K, V> junction, String id) {
            this.token = token;
            this.junction = junction;
            this.id = id;
        }

        @Override
        public int compareTo(ClusterEntry o) {
            return Long.compare(token, o.token);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterEntry that = (ClusterEntry) o;
            return token == that.token && Objects.equals(junction, that.junction) && Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(token, junction, id);
        }
    }

    public ClusterJunction(Discovery discovery) {
        this.discovery = discovery;
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            runLoop();
        }
    }

    private void runLoop() {
        discovery.discover().whenComplete((discoveryResult, throwable) -> {
            if (discoveryResult != null) {
                List<ClusterEntry> entries = this.entries.get();
                entries = entries.stream().filter(e ->
                    discoveryResult.removedNodes.stream().noneMatch(n -> e.id.equals(n.id))
                ).collect(Collectors.toCollection(ArrayList::new));
                CompletableFuture.allOf(discoveryResult.addedNodes.stream().map(n -> {
                    CompletableFuture<List<ClusterEntry>> future = new CompletableFuture<>();
                    try {
                        AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
                        socket.connect(n.address, null, new CompletionHandler<>() {
                            @Override
                            public void completed(Void result, Object attachment) {
                                RemoteJunction<K, V> junct = new RemoteJunction<>(null, null, n -> {
                                });
                                junct.tokens().whenComplete((tokens, ex) -> {
                                    if (ex != null) {
                                        future.completeExceptionally(ex);
                                    } else {
                                        future.complete(tokens.stream().map(t -> new ClusterEntry(t, junct, n.id)).collect(Collectors.toList()));
                                    }
                                });
                            }

                            @Override
                            public void failed(Throwable exc, Object attachment) {
                                future.completeExceptionally(exc);
                            }
                        });
                    } catch (IOException e) {
                        future.completeExceptionally(e);
                    }
                    return future;
                }).toArray(CompletableFuture[]::new));
            }
            if (running.get()) {
                runLoop();
            }
        });
    }

    @Override
    public CompletableFuture<Boolean> send(K id, V value, long timeout, TimeUnit timeoutUnit) {
        return null;
    }

    @Override
    public CompletableFuture<V> recv(K id, long timout, TimeUnit timeoutUnit) {
        return null;
    }
}
