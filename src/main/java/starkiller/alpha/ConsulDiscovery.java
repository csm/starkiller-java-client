package starkiller.alpha;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class ConsulDiscovery implements Discovery {
    private final HttpClient httpClient;
    private final URI consulUri;
    private final String serviceName;
    private final AtomicReference<String> token = new AtomicReference<>(null);
    private final AtomicReference<SortedSet<Node>> nodes = new AtomicReference<>(Collections.emptySortedSet());
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static final URI DEFAULT_URI;
    public static final String DEFAULT_SERVICE_NAME = "skywalker";

    static {
        try {
            DEFAULT_URI = new URI("http://localhost:8500");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ConsulHealthEntry {
        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Node {
            @JsonProperty("ID") public String id;
            @JsonProperty("Node") public String node;
            @JsonProperty("Address") public String address;
            @JsonProperty("Datacenter") public String datacenter;
            @JsonProperty("CreateIndex") public Integer createIndex;
            @JsonProperty("ModifyIndex") public Integer modifyIndex;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Service {
            @JsonProperty("ID") public String id;
            @JsonProperty("Service") public String service;
            @JsonProperty("Tags") public List<String> tags;
            @JsonProperty("Address") public String address;
            @JsonProperty("Port") public Integer port;
            @JsonProperty("CreateIndex") public Integer createIndex;
            @JsonProperty("ModifyIndex") public Integer modifyIndex;
        }

        @JsonIgnoreProperties(ignoreUnknown = true)
        public static class Check {
            @JsonProperty("Node") public String node;
            @JsonProperty("CheckID") public String checkId;
            @JsonProperty("Name") public String name;
            @JsonProperty("Status") public String status;
            @JsonProperty("CreateIndex") public Integer createIndex;
            @JsonProperty("ModifyIndex") public Integer modifyIndex;
        }

        @JsonProperty("Node")
        public Node node;

        @JsonProperty("Service")
        public Service service;

        @JsonProperty("Checks")
        public List<Check> checks;
    }

    public ConsulDiscovery() {
        this(DEFAULT_URI);
    }

    public ConsulDiscovery(URI consulUri) {
        this(consulUri, DEFAULT_SERVICE_NAME);
    }

    public ConsulDiscovery(URI consulUri, String serviceName) {
        this.httpClient = HttpClient.newHttpClient();
        this.consulUri = consulUri;
        this.serviceName = serviceName;
    }

    @Override
    public CompletableFuture<DiscoveryResult> discover() {
        String path = "/v1/health/service/" + serviceName + "?wat=60s&passing=true";
        String index = this.token.get();
        if (index != null) {
            path += "&index=" + index;
        }
        URI uri = consulUri.resolve(path);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(uri)
                .GET()
                .build();
        CompletableFuture<HttpResponse<byte[]>> requestFuture = this.httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofByteArray());
        return requestFuture.thenApply(response -> {
            if (response.statusCode() == 200) {
                try {
                    JsonParser parser = objectMapper.createParser(response.body());
                    if (parser.nextToken() == JsonToken.START_ARRAY) {
                        List<ConsulHealthEntry> entries = new LinkedList<>();
                        while (parser.nextToken() != JsonToken.END_ARRAY) {
                            entries.add(parser.readValueAs(ConsulHealthEntry.class));
                        }
                        SortedSet<Node> currentNodes = nodes.get();
                        SortedSet<Node> nextNodes = currentNodes.stream().filter(n -> {
                            for (ConsulHealthEntry e : entries) {
                                if (n.id.equals(e.node.id)) {
                                    return true;
                                }
                            }
                            return false;
                        }).collect(Collectors.toCollection(TreeSet::new));
                        SortedSet<Node> addedNodes = entries.stream().filter(e -> {
                            for (Node n : nextNodes) {
                                if (n.id.equals(e.node.id)) {
                                    return false;
                                }
                            }
                            return true;
                        }).map(e -> new Node(e.node.id, new InetSocketAddress(e.node.address, e.service.port)))
                            .collect(Collectors.toCollection(TreeSet::new));
                        nextNodes.addAll(addedNodes);
                        SortedSet<Node> removedNodes = new TreeSet<>(currentNodes);
                        removedNodes.removeAll(nextNodes);
                        nodes.compareAndSet(currentNodes, nextNodes);
                        response.headers().firstValue("X-Consul-Index").stream().forEach(ConsulDiscovery.this.token::set);
                        return new DiscoveryResult(nextNodes, addedNodes, removedNodes);
                    } else {
                        throw new RuntimeException("expected JSON array response");
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new RuntimeException(String.format("consul failed with status %d", response.statusCode()));
            }
        });
    }
}
