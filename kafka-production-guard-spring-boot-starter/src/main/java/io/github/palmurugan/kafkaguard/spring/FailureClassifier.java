package io.github.palmurugan.kafkaguard.spring;

import io.github.palmurugan.kafkaguard.NonRetryableException;
import org.springframework.core.convert.ConversionException;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;

import java.util.ArrayList;
import java.util.List;

/** Single source of truth for "is this failure poison (never retry) or transient?". */
public final class FailureClassifier {

    private static final List<Class<? extends Throwable>> BUILT_IN = List.of(
            DeserializationException.class,
            MessageConversionException.class,
            ConversionException.class,
            MethodArgumentResolutionException.class,
            ClassCastException.class,
            NonRetryableException.class);

    private final List<Class<? extends Throwable>> nonRetryable = new ArrayList<>(BUILT_IN);

    public FailureClassifier(List<Class<? extends Throwable>> extra) {
        nonRetryable.addAll(extra);
    }

    public List<Class<? extends Throwable>> nonRetryable() {
        return List.copyOf(nonRetryable);
    }

    public boolean isPoison(Throwable failure) {
        for (Throwable t = failure; t != null; t = t.getCause() == t ? null : t.getCause()) {
            for (Class<? extends Throwable> type : nonRetryable) {
                if (type.isInstance(t)) {
                    return true;
                }
            }
        }
        return false;
    }
}
