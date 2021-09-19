package starkiller;

import clojure.java.api.*;
import clojure.lang.IFn;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import starkiller.RemoteJunction;
import starkiller.TimeoutException;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestRemoteJunction {
    int port = 0;
    RemoteJunction<String, String> junction;

    @Before
    public void setup() throws IOException, ExecutionException, InterruptedException {
        IFn require = Clojure.var("clojure.core", "require");
        require.invoke(Clojure.read("starkiller.server.testing"));
        IFn test_server = Clojure.var("starkiller.server.testing", "test-server");
        Map server = (Map) test_server.invoke();
        port = ((InetSocketAddress) ((AsynchronousServerSocketChannel) server.get(Clojure.read(":socket"))).getLocalAddress()).getPort();
        AsynchronousSocketChannel socket = AsynchronousSocketChannel.open();
        socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port)).get();
        junction = new RemoteJunction<>(socket);
        junction.start();
    }

    @After
    public void teardown() throws IOException {
        junction.stop();
        junction.close();
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
    public void testRecvTimeout() throws ExecutionException, InterruptedException {
        try {
            junction.recv("foo", 5, TimeUnit.SECONDS).get();
        } catch (ExecutionException e) {
            if (!(e.getCause() instanceof TimeoutException)) {
                throw e;
            }
        }
    }

    @Test
    public void testSendRecv() throws ExecutionException, InterruptedException {
        CompletableFuture<Boolean> send = junction.send("foo", "bar", 5, TimeUnit.SECONDS);
        CompletableFuture<String> recv = junction.recv("foo", 5, TimeUnit.SECONDS);
        assertEquals(true, send.get());
        assertEquals("bar", recv.get());
    }

    @Test
    public void testRecvSend() throws ExecutionException, InterruptedException {
        CompletableFuture<String> recv1 = junction.recv("foo", 5, TimeUnit.SECONDS);
        CompletableFuture<String> recv2 = junction.recv("foo", 5, TimeUnit.SECONDS);
        CompletableFuture<Boolean> send = junction.send("foo", "baz", 5, TimeUnit.SECONDS);
        assertEquals("baz", recv1.get());
        assertEquals("baz", recv2.get());
        assertEquals(true, send.get());
    }

    public static class Data {
        @JsonProperty
        public String aString;

        @JsonProperty
        public float aFloat;

        @JsonProperty
        public long aLong;

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
    public void testSendRecvObject() throws IOException, ExecutionException, InterruptedException {
        try (AsynchronousSocketChannel socket = AsynchronousSocketChannel.open()) {
            socket.connect(new InetSocketAddress(InetAddress.getLoopbackAddress(), port));
            try (RemoteJunction<String, Data> j = new RemoteJunction<>(socket)) {
                Data d1 = new Data("foo", (float) 3.14159, 42);
                CompletableFuture<Boolean> send = j.send("test", d1, 5, TimeUnit.SECONDS);
                CompletableFuture<Data> recv = j.recv("test", 5, TimeUnit.SECONDS);
                assertEquals(d1, recv.get());
                assertTrue(send.get());
            }
        }
    }
}
