package starkiller.alpha;

import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CompletableFuture;

public interface Discovery {
    final class Node implements Comparable<Node> {
        public final String id;
        public final InetSocketAddress address;
        public final SortedSet<Long> tokens;

        public Node(String id, InetSocketAddress address) {
            this(id, address, Collections.emptySortedSet());
        }

        public Node(String id, InetSocketAddress address, SortedSet<Long> tokens) {
            this.id = id;
            this.address = address;
            this.tokens = Collections.unmodifiableSortedSet(tokens);
        }

        public Node withTokens(SortedSet<Long> tokens) {
            return new Node(id, address, tokens);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Node node = (Node) o;
            return Objects.equals(id, node.id) && Objects.equals(address, node.address) && Objects.equals(tokens, node.tokens);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, address, tokens);
        }

        @Override
        public String toString() {
            return "Node{" +
                    "id='" + id + '\'' +
                    ", address=" + address +
                    ", tokens=" + tokens +
                    '}';
        }

        @Override
        public int compareTo(Node o) {
            int cmp = address.getAddress().getHostAddress().compareTo(o.address.getAddress().getHostAddress());
            if (cmp == 0) {
                return Integer.compare(address.getPort(), o.address.getPort());
            }
            return cmp;
        }
    }

    final class DiscoveryResult {
        public final Set<Node> nodes;
        public final Set<Node> addedNodes;
        public final Set<Node> removedNodes;

        public DiscoveryResult(Set<Node> nodes, Set<Node> addedNodes, Set<Node> removedNodes) {
            this.nodes = nodes;
            this.addedNodes = addedNodes;
            this.removedNodes = removedNodes;
        }

        @Override
        public String toString() {
            return "DiscoveryResult{" +
                    "nodes=" + nodes +
                    ", addedNodes=" + addedNodes +
                    ", removedNodes=" + removedNodes +
                    '}';
        }
    }

    CompletableFuture<DiscoveryResult> discover();
}
