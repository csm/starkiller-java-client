package starkiller;

import java.io.IOException;

/**
 * A timeout returned when the user-supplied timeout to
 * send or recv elapses without success.
 */
public class TimeoutException extends IOException {
    public TimeoutException() {
        super("method call timed out");
    }
}
