package io.starburst.verifier;

import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.starburst.verifier.Utils.findResources;
import static io.starburst.verifier.Utils.readResource;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Suites
{
    private static final String RESOURCE_ROOT = "suites/";
    private static final String RESOURCE_EXTENSION = ".suite";
    private static final Pattern RESOURCE_PATTERN = Pattern.compile(format("%s.*%s", Pattern.quote(RESOURCE_ROOT), Pattern.quote(RESOURCE_EXTENSION)));
    private static final Pattern SUITE_DEFINITION_PATTERN = Pattern.compile("([a-zA-Z0-9\\/_]+)(\\sx(\\d+))?");

    private final Queries queries;

    @Inject
    public Suites(Queries queries)
    {
        this.queries = requireNonNull(queries, "queries is null");
    }

    public Optional<Suite> get(String id)
    {
        Optional<String> suiteDefinition = readResource(RESOURCE_ROOT + id + RESOURCE_EXTENSION);
        if (suiteDefinition.isEmpty()) {
            return Optional.empty();
        }
        List<SuiteEntry> entries = parseSuiteDefinition(id, suiteDefinition.get());
        ImmutableList.Builder<Query> result = ImmutableList.builder();
        for (SuiteEntry entry : entries) {
            Query query = queries.get(entry.getQueryId()).orElseThrow(() ->
                    new IllegalArgumentException(format("Error creating suite %s. Query %s not found.", id, entry.getQueryId())));
            for (int i = 0; i < entry.getRepetitions(); i++) {
                result.add(query);
            }
        }
        return Optional.of(new Suite(id, result.build()));
    }

    private List<SuiteEntry> parseSuiteDefinition(String id, String definition)
    {
        ImmutableList.Builder<SuiteEntry> result = ImmutableList.builder();
        AtomicInteger lineNumber = new AtomicInteger(1);
        definition.lines().forEach(line -> {
            Matcher matcher = SUITE_DEFINITION_PATTERN.matcher(line);
            if (!matcher.matches()) {
                throw parsingException(id, lineNumber.get(), line);
            }
            String queryId = matcher.group(1);
            int repetitions = 1;
            String repetitionsString = matcher.group(3);
            if (repetitionsString != null) {
                try {
                    repetitions = Integer.parseInt(repetitionsString);
                }
                catch (NumberFormatException e) {
                    throw parsingException(id, lineNumber.get(), line);
                }
            }
            result.add(new SuiteEntry(queryId, repetitions));
            lineNumber.incrementAndGet();
        });
        return result.build();
    }

    private IllegalArgumentException parsingException(String suiteId, int lineNumber, String line)
    {
        return new IllegalArgumentException(format("Error parsing suite definition '%s' at line %s: '%s'", suiteId, lineNumber, line));
    }

    public List<String> getAllIds()
    {
        Set<String> resources = findResources(RESOURCE_PATTERN);
        return resources.stream()
                .map(Suites::extractSuiteIdFromResourcePath)
                .collect(toImmutableList());
    }

    private static String extractSuiteIdFromResourcePath(String resourcePath)
    {
        if (!resourcePath.startsWith(RESOURCE_ROOT) || !resourcePath.endsWith(RESOURCE_EXTENSION)) {
            throw new IllegalArgumentException("Unexpected resource path: " + resourcePath);
        }
        return resourcePath.substring(RESOURCE_ROOT.length(), resourcePath.length() - RESOURCE_EXTENSION.length());
    }

    private static class SuiteEntry
    {
        private final String queryId;
        private final int repetitions;

        private SuiteEntry(String queryId, int repetitions)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.repetitions = repetitions;
        }

        public String getQueryId()
        {
            return queryId;
        }

        public int getRepetitions()
        {
            return repetitions;
        }
    }
}
