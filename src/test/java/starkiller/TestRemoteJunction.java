package starkiller;

import clojure.java.api.*;
import clojure.lang.IFn;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestRemoteJunction {
    static final Logger logger = LoggerFactory.getLogger(TestRemoteJunction.class);
    int port = 0;
    RemoteJunction<String, String> junction;
    Map server;

    @Before
    public void setup() throws IOException, ExecutionException, InterruptedException {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("starkiller.server.testing"));
        IFn test_server = Clojure.var("starkiller.server.testing", "test-server");
        server = (Map) test_server.invoke();
        port = ((InetSocketAddress) ((AsynchronousServerSocketChannel) server.get(Clojure.read(":socket"))).getLocalAddress()).getPort();
        junction = new RemoteJunction<>(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), String.class);
        junction.start();
    }

    @After
    public void teardown() throws IOException {
        junction.stop();
        junction.close();
        ((AsynchronousServerSocketChannel) server.get(Clojure.read(":socket"))).close();
    }

    @Test
    public void testSendTimeout() throws InterruptedException, ExecutionException {
        try {
            junction.send("foo", "bar", 5, TimeUnit.SECONDS).get();
            throw new RuntimeException("call should have timed out");
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TimeoutException)) {
                throw e;
            }
        }
    }

    @Test
    public void testRecvTimeout() throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
        try {
            junction.recv("foo", 5, TimeUnit.SECONDS).get(10, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TimeoutException)) {
                throw e;
            }
        }
    }

    @Test
    public void testSendRecv() throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
        CompletableFuture<Boolean> send = junction.send("foo", "bar", 5, TimeUnit.SECONDS);
        CompletableFuture<String> recv = junction.recv("foo", 5, TimeUnit.SECONDS);
        assertEquals(true, send.get(10, TimeUnit.SECONDS));
        assertEquals("bar", recv.get(10, TimeUnit.SECONDS));
    }

    @Test
    public void testRecvSend() throws ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
        CompletableFuture<String> recv1 = junction.recv("foo", 5, TimeUnit.SECONDS);
        CompletableFuture<String> recv2 = junction.recv("foo", 5, TimeUnit.SECONDS);
        CompletableFuture<Boolean> send = junction.send("foo", "baz", 5, TimeUnit.SECONDS);
        logger.info("waiting on recv1:{} recv2:{} send:{}", recv1, recv2, send);
        assertEquals("baz", recv1.get(10, TimeUnit.SECONDS));
        assertEquals("baz", recv2.get(10, TimeUnit.SECONDS));
        assertEquals(true, send.get(10, TimeUnit.SECONDS));
    }

    public static class Data {
        @JsonProperty
        public String aString;

        @JsonProperty
        public float aFloat;

        @JsonProperty
        public long aLong;

        public Data() {
        }

        public Data(String aString, float aFloat, long aLong) {
            this.aString = aString;
            this.aFloat = aFloat;
            this.aLong = aLong;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Data data = (Data) o;
            return Float.compare(data.aFloat, aFloat) == 0 && aLong == data.aLong && Objects.equals(aString, data.aString);
        }

        @Override
        public int hashCode() {
            return Objects.hash(aString, aFloat, aLong);
        }
    }

    @Test
    public void testSendRecvObject() throws IOException, ExecutionException, InterruptedException, java.util.concurrent.TimeoutException {
        try (RemoteJunction<String, Data> j = new RemoteJunction<>(new InetSocketAddress(InetAddress.getLoopbackAddress(), port), Data.class)) {
            j.start();
            Data d1 = new Data("foo", (float) 3.14159, 42);
            CompletableFuture<Boolean> send = j.send("test", d1, 5, TimeUnit.SECONDS);
            CompletableFuture<Data> recv = j.recv("test", 5, TimeUnit.SECONDS);
            assertEquals(d1, recv.get(10, TimeUnit.SECONDS));
            assertTrue(send.get(10, TimeUnit.SECONDS));
        }
    }
}
