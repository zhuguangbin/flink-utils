package org.apache.flink.formats.avro.registry.confluent;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.Optional;
import java.util.function.Predicate;

public class FlinkTestUtils {

    /**
     * Checks for a {@link Throwable} that matches by class and message.
     */
    public static Matcher<Throwable> containsCause(Throwable failureCause) {
        return new ContainsCauseMatcher(failureCause);
    }

    private static final class ContainsCauseMatcher extends TypeSafeDiagnosingMatcher<Throwable> {

        private final Throwable failureCause;

        private ContainsCauseMatcher(Throwable failureCause) {
            this.failureCause = failureCause;
        }

        @Override
        protected boolean matchesSafely(Throwable throwable, Description description) {
            final Optional<Throwable> optionalCause = findThrowable(
                    throwable,
                    cause ->
                            cause.getClass() == failureCause.getClass() &&
                                    cause.getMessage().equals(failureCause.getMessage()));

            if (!optionalCause.isPresent()) {
                description
                        .appendText("The throwable ")
                        .appendValue(throwable)
                        .appendText(" does not contain the expected failure cause ")
                        .appendValue(failureCause);
            }

            return optionalCause.isPresent();
        }

        @Override
        public void describeTo(Description description) {
            description
                    .appendText("Expected failure cause is ")
                    .appendValue(failureCause);
        }

        // copied from flink-core to not mess up the dependency design too much, just for a little
        // utility method
        private static Optional<Throwable> findThrowable(
                Throwable throwable,
                Predicate<Throwable> predicate) {
            if (throwable == null || predicate == null) {
                return Optional.empty();
            }

            Throwable t = throwable;
            while (t != null) {
                if (predicate.test(t)) {
                    return Optional.of(t);
                } else {
                    t = t.getCause();
                }
            }

            return Optional.empty();
        }
    }
}
