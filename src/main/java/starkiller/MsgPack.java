package starkiller;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

class MsgPack<K, V> {
    private final Class<? extends V> valueClass;
    private final ObjectMapper objectMapper;
    private static final Logger logger = LoggerFactory.getLogger(MsgPack.class);

    MsgPack(ObjectMapper objectMapper, Class<? extends V> valueClass)
    {
        this.objectMapper = objectMapper;
        this.valueClass = valueClass;
    }

    private String unpackString(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xFF;
        int bytesLength;
        if (b >= 0b10100000 && b <= 0b10111111) {
            bytesLength = b & 0b11111;
        } else if (b == 0xd9) {
            bytesLength = buffer.get() & 0xFF;
        } else if (b == 0xda) {
            bytesLength = buffer.getShort() & 0xFFFF;
        } else if (b == 0xdb) {
            bytesLength = buffer.getInt();
        } else {
            throw new IOException(String.format("invalid byte %02x in stream", b));
        }
        byte[] bytes = new byte[bytesLength];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private long unpackLong(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xFF;
        if (b <= 0x7f) {
            return b;
        } else if (b >= 0b11100000) {
            return b | 0xFFFFFFFFFFFFFF00L;
        } else if (b == 0xcc) {
            return buffer.get() & 0xFF;
        } else if (b == 0xcd) {
            return buffer.getShort() & 0xFFFF;
        } else if (b == 0xce) {
            return buffer.getInt() & 0xFFFFFFFFL;
        } else if (b == 0xcf || b == 0xd3) {
            return buffer.getLong();
        } else if (b == 0xd0) {
            return buffer.get();
        } else if (b == 0xd1) {
            return buffer.getShort();
        } else if (b == 0xd2) {
            return buffer.getInt();
        } else {
            throw new IOException(String.format("invalid byte %02x in stream", b));
        }
    }

    private boolean peekTimeout(ByteBuffer buffer) {
        buffer = buffer.asReadOnlyBuffer().slice();
        int b = buffer.get() & 0xFF;
        if (b == 0xd4) {
            b = buffer.get() & 0xFF;
            return b == (byte) 'T';
        }
        return false;
    }

    private int unpackArrayStart(ByteBuffer buffer) throws IOException {
        int b = buffer.get() & 0xFF;
        int arrayLength;
        if (b >= 0b1001000 && b <= 0b10011111) {
            arrayLength = b & 0xF;
        } else if (b == 0xdc) {
            arrayLength = buffer.getShort() & 0xFFFF;
        } else if (b == 0xdd) {
            arrayLength = buffer.getInt();
        } else {
            throw new IOException(String.format("invalid byte %02x in stream", b));
        }
        return arrayLength;
    }

    RemoteJunction.Response<V> unpackResponse(ByteBuffer buffer) throws IOException {
        int arrayLength = unpackArrayStart(buffer);
        if (arrayLength != 3) {
            throw new IOException(String.format("invalid response array length: %d", arrayLength));
        }
        String method = unpackString(buffer);
        long messageId = unpackLong(buffer);
        boolean isTimeout = peekTimeout(buffer);
        switch (method) {
            case ":recv!": {
                if (isTimeout) {
                    buffer.position(buffer.position() + 3);
                    return new RemoteJunction.TimeoutResponse<>(messageId);
                } else {
                    V value = objectMapper.readValue(new ByteBufferBackedInputStream(buffer), valueClass);
                    return new RemoteJunction.RecvResponse<>(messageId, value);
                }
            }
            case ":send!": {
                if (isTimeout) {
                    buffer.position(buffer.position() + 3);
                    return new RemoteJunction.TimeoutResponse<>(messageId);
                } else {
                    Boolean value = objectMapper.readValue(new ByteBufferBackedInputStream(buffer), Boolean.class);
                    return new RemoteJunction.SendResponse<>(messageId, value);
                }
            }
            case ":tokens": {
                int tokenCount = unpackArrayStart(buffer);
                List<Long> tokens = new ArrayList<>(tokenCount);
                for (int i = 0; i < tokenCount; i++) {
                    tokens.add(unpackLong(buffer));
                }
                return new RemoteJunction.TokenResponse<>(messageId, tokens);
            }
            default:
                throw new IOException(String.format("invalid method name %s", method));
        }
    }

    byte[] packRequest(RemoteJunction.Request<K, V> request) throws IOException {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        if (request instanceof RemoteJunction.SendRequest) {
            RemoteJunction.SendRequest<K, V> sr = (RemoteJunction.SendRequest<K, V>) request;
            JsonGenerator gen = objectMapper.createGenerator(bout);
            gen.writeStartArray(5);
            gen.writeString(":send!");
            gen.writeNumber(request.messageId);
            gen.writeNumber(sr.timeout);
            gen.writeObject(sr.id);
            gen.writeObject(sr.value);
            gen.writeEndArray();
            gen.flush();
            gen.close();
        } else if (request instanceof RemoteJunction.RecvRequest) {
            RemoteJunction.RecvRequest<K, V> rr = (RemoteJunction.RecvRequest<K, V>) request;
            JsonGenerator gen = objectMapper.createGenerator(bout);
            gen.writeStartArray(4);
            gen.writeString(":recv!");
            gen.writeNumber(rr.messageId);
            gen.writeNumber(rr.timeout);
            gen.writeObject(rr.id);
            gen.writeEndArray();
            gen.flush();
            gen.close();
        } else if (request instanceof RemoteJunction.TokenRequest) {
            JsonGenerator gen = objectMapper.createGenerator(bout);
            gen.writeStartArray(2);
            gen.writeString(":tokens");
            gen.writeNumber(request.messageId);
            gen.writeEndArray();
            gen.flush();
            gen.close();
        } else {
            throw new IllegalArgumentException("unsupported class " + (request != null ? request.getClass().getName() : "null"));
        }
        return bout.toByteArray();
    }
}
