package io.trino.verifier.queries;

import io.airlift.json.JsonCodec;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static io.trino.verifier.queries.Utils.findResources;
import static io.trino.verifier.queries.Utils.readResource;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Queries
{
    private static final String RESOURCE_ROOT = "queries/";
    private static final String QUERY_EXTENSION = ".sql";
    private static final Pattern RESOURCE_PATTERN = Pattern.compile(format("%s.*%s", Pattern.quote(RESOURCE_ROOT), Pattern.quote(QUERY_EXTENSION)));
    private static final String SESSION_PROPERTIES_SUFFIX = "_session_properties.json";

    private final JsonCodec<Map<String, String>> sessionPropertiesCodec;

    public Queries()
    {
        this(mapJsonCodec(String.class, String.class));
    }

    @Inject
    public Queries(JsonCodec<Map<String, String>> sessionPropertiesCodec)
    {
        this.sessionPropertiesCodec = requireNonNull(sessionPropertiesCodec, "sessionPropertiesCodec is null");
    }

    public Optional<Query> get(String id)
    {
        Optional<String> sql = readResource(RESOURCE_ROOT + id + QUERY_EXTENSION);
        if (sql.isEmpty()) {
            return Optional.empty();
        }
        Map<String, String> sessionProperties = readResource(RESOURCE_ROOT + id + SESSION_PROPERTIES_SUFFIX)
                .map(sessionPropertiesCodec::fromJson)
                .orElse(Map.of());
        return Optional.of(new Query(id, sql.get(), sessionProperties));
    }

    public List<String> getAllIds()
    {
        Set<String> resources = findResources(RESOURCE_PATTERN);
        return resources.stream()
                .map(Queries::extractQueryIdFromResourcePath)
                .collect(toImmutableList());
    }

    private static String extractQueryIdFromResourcePath(String resourcePath)
    {
        if (!resourcePath.startsWith(RESOURCE_ROOT) || !resourcePath.endsWith(QUERY_EXTENSION)) {
            throw new IllegalArgumentException("Unexpected resource path: " + resourcePath);
        }
        return resourcePath.substring(RESOURCE_ROOT.length(), resourcePath.length() - QUERY_EXTENSION.length());
    }
}
