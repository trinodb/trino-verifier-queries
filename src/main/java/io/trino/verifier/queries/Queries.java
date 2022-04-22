package io.trino.verifier.queries;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.verifier.queries.Utils.findResources;
import static io.trino.verifier.queries.Utils.readResource;
import static java.lang.String.format;

public class Queries
{
    private static final String RESOURCE_ROOT = "queries/";
    private static final String RESOURCE_EXTENSION = ".sql";
    private static final Pattern RESOURCE_PATTERN = Pattern.compile(format("%s.*%s", Pattern.quote(RESOURCE_ROOT), Pattern.quote(RESOURCE_EXTENSION)));

    public Optional<Query> get(String id)
    {
        return readResource(RESOURCE_ROOT + id + RESOURCE_EXTENSION).map(sql -> new Query(id, sql));
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
        if (!resourcePath.startsWith(RESOURCE_ROOT) || !resourcePath.endsWith(RESOURCE_EXTENSION)) {
            throw new IllegalArgumentException("Unexpected resource path: " + resourcePath);
        }
        return resourcePath.substring(RESOURCE_ROOT.length(), resourcePath.length() - RESOURCE_EXTENSION.length());
    }
}
