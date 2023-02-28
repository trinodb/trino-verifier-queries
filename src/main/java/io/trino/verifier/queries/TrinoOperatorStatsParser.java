package io.trino.verifier.queries;

import com.google.common.collect.ImmutableMap;
import com.mysql.jdbc.Driver;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.json.JsonCodec.listJsonCodec;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class TrinoOperatorStatsParser
{
    private static final String QUERY = "SELECT source, operator_summaries_json FROM trino_queries WHERE query_id IN (SELECT test_query_id FROM verifier_run_summary WHERE run_id = '%s') ORDER BY query_id";

    private TrinoOperatorStatsParser() {}

    public static void main(String[] args)
            throws SQLException
    {
        try {
            DriverManager.registerDriver(new Driver());
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }

        String url = args[0];
        String runId = args[1];

        Map<String, List<OperatorStats>> operatorStats = getOperatorStats(url, runId);
        Map<String, Map<String, Long>> cpuTime = operatorStats.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> getCpuByOperatorTypeInMillis(entry.getValue())));
        Map<String, Map<String, Map<String, Long>>> grouped = cpuTime.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> groupOperators(entry.getValue())));

        grouped.forEach((query, groupedTimings) -> {
            long totalForQuery = groupedTimings.values().stream()
                    .flatMap(map -> map.values().stream())
                    .mapToLong(timing -> timing)
                    .sum();
            System.out.println(query + ": " + new Duration(totalForQuery, MILLISECONDS).convertToMostSuccinctTimeUnit());
            groupedTimings.forEach((group, timings) -> {
                long totalForGroup = timings.values().stream()
                        .mapToLong(timing -> timing)
                        .sum();
                System.out.printf("  %s=%s\n", group, new Duration(totalForGroup, MILLISECONDS).convertToMostSuccinctTimeUnit());
                timings.entrySet().stream()
                        .filter(timing -> timing.getValue() > 0)
                        .forEach(timing -> System.out.printf("    %s=%s\n", timing.getKey(), new Duration(timing.getValue(), MILLISECONDS).convertToMostSuccinctTimeUnit()));
            });
        });
        long totalOverall = grouped.values().stream()
                .flatMap(map -> map.values().stream())
                .flatMap(map -> map.values().stream())
                .mapToLong(timing -> timing)
                .sum();
        System.out.println("Total: " + new Duration(totalOverall, MILLISECONDS).convertToMostSuccinctTimeUnit());
        getTotal(operatorStats).forEach((group, timings) -> {
            long totalForGroup = timings.values().stream()
                    .mapToLong(timing -> timing)
                    .sum();
            System.out.printf("  %s=%s\n", group, new Duration(totalForGroup, MILLISECONDS).convertToMostSuccinctTimeUnit());
            timings.entrySet().stream()
                    .filter(timing -> timing.getValue() > 0)
                    .forEach(timing -> System.out.printf("    %s=%s\n", timing.getKey(), new Duration(timing.getValue(), MILLISECONDS).convertToMostSuccinctTimeUnit()));
        });
    }

    private static Map<String, List<OperatorStats>> getOperatorStats(String url, String runId)
            throws SQLException
    {
        JsonCodec<List<OperatorStats>> codec = listJsonCodec(OperatorStats.class);
        ImmutableMap.Builder<String, List<OperatorStats>> result = ImmutableMap.builder();
        try (Connection connection = DriverManager.getConnection(url)) {
            connection.setSchema("verifier");
            try (Statement statement = connection.createStatement();
                    ResultSet rs = statement.executeQuery(format(QUERY, runId))) {
                while (rs.next()) {
                    String source = rs.getString("source");
                    String operatorStatsJson = rs.getString("operator_summaries_json");
                    List<OperatorStats> operatorStats = codec.fromJson(operatorStatsJson);
                    result.put(source, operatorStats);
                }
            }
        }
        return result.buildOrThrow();
    }

    private static Map<String, Map<String, Long>> groupOperators(Map<String, Long> stats)
    {
        Map<String, Map<String, Long>> result = new HashMap<>();
        stats.forEach((operator, cpu) -> {
            String group = getGroup(operator);
            result.computeIfAbsent(group, key -> new LinkedHashMap<>()).put(operator, cpu);
        });
        return result.entrySet().stream()
                .sorted(Comparator.<Map.Entry<String, Map<String, Long>>, Long>comparing(entry -> entry.getValue().values().stream().mapToLong(value -> value).sum()).reversed())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static Map<String, Long> getCpuByOperatorTypeInMillis(List<OperatorStats> allStats)
    {
        Map<String, Long> result = new HashMap<>();
        for (OperatorStats stats : allStats) {
            long cpu = getCpuTimeInMillis(stats);
            result.merge(stats.getOperatorType(), cpu, Long::sum);
        }
        return result.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static long getCpuTimeInMillis(OperatorStats stats)
    {
        return stats.getAddInputCpu().toMillis() + stats.getGetOutputCpu().toMillis() + stats.getFinishCpu().toMillis();
    }

    private static Map<String, Map<String, Long>> getTotal(Map<String, List<OperatorStats>> operatorStats)
    {
        Map<String, Long> cpuByOperatorTypeInMillis = getCpuByOperatorTypeInMillis(operatorStats.values().stream()
                .flatMap(List::stream)
                .collect(toList()));
        return groupOperators(cpuByOperatorTypeInMillis);
    }

    private static String getGroup(String operatorType)
    {
        switch (operatorType) {
            case "PartitionedOutputOperator":
            case "TaskOutputOperator":
            case "ExchangeOperator":
            case "LocalExchangeSinkOperator":
            case "LocalExchangeSourceOperator":
                return "Exchange";
            case "LookupJoinOperator":
            case "HashBuilderOperator":
            case "DynamicFilterSourceOperator":
            case "LookupOuterOperator":
            case "NestedLoopJoinOperator":
            case "AssignUniqueIdOperator":
            case "HashSemiJoinOperator":
            case "NestedLoopBuildOperator":
                return "Join";
            case "HashAggregationOperator":
            case "GroupIdOperator":
            case "AggregationOperator":
            case "MarkDistinctOperator":
                return "Aggregation";
            case "ScanFilterAndProjectOperator":
            case "FilterAndProjectOperator":
            case "TableScanOperator":
                return "ScanFilterProject";
            default:
                return "Other";
        }
    }
}
