package io.starburst.verifier;

import com.google.common.io.Resources;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ConfigurationBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.io.Resources.getResource;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class Utils
{
    private Utils() {}

    public static Optional<String> readResource(String resource)
    {
        Optional<URL> resourceUrl = getResourceUrl(resource);
        if (resourceUrl.isEmpty()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Resources.toString(resourceUrl.get(), UTF_8));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Optional<URL> getResourceUrl(String resource)
    {
        try {
            return Optional.of(getResource(resource));
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    public static Set<String> findResources(Pattern pattern)
    {
        Reflections reflections = new Reflections(new ConfigurationBuilder()
                .forPackage(Utils.class.getPackage().getName())
                .setScanners(Scanners.Resources)
                .filterInputsBy(resource -> pattern.matcher(resource).matches()));
        return reflections.getResources(".*");
    }
}
