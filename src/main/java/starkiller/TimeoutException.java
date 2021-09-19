package starkiller;

import java.io.IOException;

public class TimeoutException extends IOException {
    public TimeoutException() {
        super("method call timed out");
    }
}
