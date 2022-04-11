package io.starburst.verifier;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.concurrent.MoreFutures;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.io.File;
import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.common.util.concurrent.Uninterruptibles.awaitTerminationUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.stream.Collectors.joining;
import static picocli.CommandLine.ScopeType.INHERIT;

@Command(name = "schemas")
public class SchemasCommand
{
    @Spec
    private CommandSpec spec;

    private File configFile;

    @Option(
            names = "--config",
            paramLabel = "<file>",
            required = true,
            scope = INHERIT,
            description = "Configuration file",
            order = 0)
    private void setConfigFile(File configFile)
    {
        requireNonNull(configFile, "configFile is null");
        if (!configFile.exists() || !configFile.isFile()) {
            throw new ParameterException(spec.commandLine(), format("Config file does not exist: %s", configFile));
        }
        if (!configFile.canRead()) {
            throw new ParameterException(spec.commandLine(), format("Config file is not readable: %s", configFile));
        }
        this.configFile = configFile;
    }

    private TrinoClient trinoClient;
    private SchemasConfig config;

    @Command(sortOptions = false)
    public void create(
            @Option(names = "--catalog", required = true, order = 1) String catalog,
            @Option(names = "--schema", required = true, order = 2) String schema,
            @Option(names = "--overwrite", order = 3) boolean overwrite,
            @Option(names = "--bucket-count", order = 4, defaultValue = "0") int bucketCount,
            @Option(names = "--partitioned", order = 5) boolean partitioned,
            @Option(names = "--scale-factor", order = 6, defaultValue = "0.1") BigDecimal scaleFactor,
            @Option(names = "--threads", order = 7, defaultValue = "1") int threads,
            @Option(names = "--schema-type", required = true, order = 8, description = "Schema type. Valid values: ${COMPLETION-CANDIDATES}") SchemaType schemaType)
    {
        init();
        schemaType.checkTrinoConfiguration(spec, trinoClient);
        List<String> catalogs = trinoClient.selectSingleStringColumn("SHOW CATALOGS");
        if (!catalogs.contains(catalog)) {
            throw new ExecutionException(spec.commandLine(), format("Catalog does not exist: %s", catalog));
        }
        List<String> schemas = trinoClient.selectSingleStringColumn(format("SHOW SCHEMAS FROM \"%s\"", catalog));
        if (schemas.contains(schema)) {
            if (!overwrite) {
                throw new ExecutionException(spec.commandLine(), format("Schema already exist: %s", schema));
            }
            dropSchema(catalog, schema);
        }
        trinoClient.executeUpdate(format("CREATE SCHEMA \"%s\".\"%s\"", catalog, schema));
        ListeningExecutorService executor = listeningDecorator(newFixedThreadPool(threads, daemonThreadsNamed("executor-%s")));
        List<ListenableFuture<Void>> futures = schemaType.getTables().stream()
                .map(table -> executor.submit(() -> copyTable(
                        schemaType.getRequiredCatalog(),
                        "sf" + scaleFactor,
                        catalog,
                        schema,
                        table,
                        bucketCount > 0 ? schemaType.getBucketingScheme().get(table) : Set.of(),
                        partitioned ? schemaType.getPartitioningScheme().get(table) : Set.of(),
                        bucketCount)))
                .map(MoreFutures::asVoid)
                .collect(toImmutableList());
        ListenableFuture<List<Void>> future = allAsList(futures);
        try {
            future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (java.util.concurrent.ExecutionException e) {
            // ignore for now
        }
        // cancel all futures that are still running
        futures.forEach(f -> f.cancel(true));
        trinoClient.closeActiveConnections();
        executor.shutdownNow();
        awaitTerminationUninterruptibly(executor);
        try {
            future.get();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        catch (java.util.concurrent.ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    private void copyTable(
            String catalogFrom,
            String schemaFrom,
            String catalogTo,
            String schemaTo,
            String tableName,
            Set<String> bucketingScheme,
            Set<String> partitioningScheme,
            int bucketCount)
    {
        List<String> tableColumns = getTableColumns(catalogFrom, schemaFrom, tableName);
        String bucketing = "";
        String partitioning = "";
        String sql = format("CREATE TABLE \"%s\".\"%s\".\"%s\" ", catalogTo, schemaTo, tableName);
        if (!bucketingScheme.isEmpty() || !partitioningScheme.isEmpty()) {
            String withClause = "";
            if (!bucketingScheme.isEmpty()) {
                bucketing = bucketingScheme.stream().map(value -> "'" + value + "'").collect(joining(" , "));
                withClause += format("bucketed_by = ARRAY[%s], bucket_count = %s", bucketing, bucketCount);
            }
            if (!partitioningScheme.isEmpty()) {
                if (!withClause.isEmpty()) {
                    withClause += ", ";
                }
                ImmutableList.Builder<String> tableColumnsBuilder = ImmutableList.builder();
                tableColumns.stream()
                        .filter(column -> !partitioningScheme.contains(column))
                        .forEach(tableColumnsBuilder::add);
                // partitioning columns must come last
                partitioningScheme.forEach(tableColumnsBuilder::add);
                tableColumns = tableColumnsBuilder.build();
                partitioning = partitioningScheme.stream().map(value -> "'" + value + "'").collect(joining(" , "));
                withClause += format("partitioned_by = ARRAY[%s]", partitioning);
            }
            sql += "WITH (" + withClause + ") ";
        }
        sql += format("AS SELECT %s FROM \"%s\".\"%s\".\"%s\"", join(", ", tableColumns), catalogFrom, schemaFrom, tableName);
        System.err.println(format("Creating table %s.%s.%s based on %s.%s.%s", catalogTo, schemaTo, tableName, catalogFrom, schemaFrom, tableName)
                + (bucketing.isEmpty() ? "" : format(" bucketed on %s", bucketing))
                + (partitioning.isEmpty() ? "" : format(" partitioned on %s", partitioning)));
        ImmutableMap.Builder<String, String> sessionProperties = ImmutableMap.builder();
        if (!partitioningScheme.isEmpty() && config.isForceWritePartitioning()) {
            // force write partitioning to overcome 100 partitions per writer restriction
            // partitioning scheme is known to create ~2000 partitions for each table
            sessionProperties.put("use_preferred_write_partitioning", "true");
            sessionProperties.put("preferred_write_partitioning_min_number_of_partitions", "1");
        }
        try {
            trinoClient.executeUpdate(sql, sessionProperties.build());
        }
        catch (RuntimeException e) {
            throw new RuntimeException(format("Error creating %s.%s.%s", catalogTo, schemaTo, tableName), e);
        }
        System.err.printf("Table %s.%s.%s has been created%n", catalogTo, schemaTo, tableName);
    }

    private void dropSchema(String catalog, String schema)
    {
        List<String> tables = trinoClient.selectSingleStringColumn(format("SHOW TABLES FROM \"%s\".\"%s\"", catalog, schema));
        for (String table : tables) {
            trinoClient.executeUpdate(format("DROP TABLE \"%s\".\"%s\".\"%s\"", catalog, schema, table));
        }
        trinoClient.executeUpdate(format("DROP SCHEMA \"%s\".\"%s\"", catalog, schema));
    }

    private List<String> getTableColumns(String catalog, String schema, String table)
    {
        return trinoClient.selectSingleStringColumn(format("" +
                        "SELECT column_name " +
                        "FROM %s.information_schema.columns " +
                        "WHERE table_catalog = '%s' " +
                        "AND table_schema = '%s' " +
                        "AND table_name = '%s'",
                catalog,
                catalog,
                schema,
                table));
    }

    private void init()
    {
        verify(configFile != null, "configFile is null");
        System.setProperty("config", configFile.getAbsolutePath());

        loadTrinoDriver();

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new SchemasModule());

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector;
        try {
            injector = app.initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        trinoClient = injector.getInstance(TrinoClient.class);
        config = injector.getInstance(SchemasConfig.class);
    }

    private static void loadTrinoDriver()
    {
        try {
            Class.forName("io.trino.jdbc.TrinoDriver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public enum SchemaType
    {
        tpch(
                "tpch",
                List.of(
                        "customer",
                        "lineitem",
                        "nation",
                        "orders",
                        "part",
                        "partsupp",
                        "region",
                        "supplier"),
                ImmutableSetMultimap.<String, String>builder()
                        .putAll("orders", Set.of("o_orderkey"))
                        .putAll("lineitem", Set.of("l_orderkey"))
                        .build(),
                ImmutableSetMultimap.of()),
        tpcds(
                "tpcds",
                List.of(
                        "call_center",
                        "catalog_page",
                        "catalog_returns",
                        "catalog_sales",
                        "customer",
                        "customer_address",
                        "customer_demographics",
                        "date_dim",
                        "household_demographics",
                        "income_band",
                        "inventory",
                        "item",
                        "promotion",
                        "reason",
                        "ship_mode",
                        "store",
                        "store_returns",
                        "store_sales",
                        "time_dim",
                        "warehouse",
                        "web_page",
                        "web_returns",
                        "web_sales",
                        "web_site"),
                ImmutableSetMultimap.of(),
                ImmutableSetMultimap.<String, String>builder()
                        .putAll("catalog_returns", Set.of("cr_returned_date_sk"))
                        .putAll("catalog_sales", Set.of("cs_sold_date_sk"))
                        .putAll("store_returns", Set.of("sr_returned_date_sk"))
                        .putAll("store_sales", Set.of("ss_sold_date_sk"))
                        .putAll("web_returns", Set.of("wr_returned_date_sk"))
                        .putAll("web_sales", Set.of("ws_sold_date_sk"))
                        .build()),
        /**/;

        private final String requiredCatalog;
        private final List<String> tables;
        private final SetMultimap<String, String> bucketingScheme;
        private final SetMultimap<String, String> partitioningScheme;

        SchemaType(String requiredCatalog, List<String> tables, SetMultimap<String, String> bucketingScheme, SetMultimap<String, String> partitioningScheme)
        {
            this.requiredCatalog = requireNonNull(requiredCatalog, "requiredCatalog is null");
            this.tables = List.copyOf(requireNonNull(tables, "tables is null"));
            this.bucketingScheme = ImmutableSetMultimap.copyOf(requireNonNull(bucketingScheme, "bucketingScheme is null"));
            this.partitioningScheme = ImmutableSetMultimap.copyOf(requireNonNull(partitioningScheme, "partitioningScheme is null"));
        }

        public String getRequiredCatalog()
        {
            return requiredCatalog;
        }

        public List<String> getTables()
        {
            return tables;
        }

        public SetMultimap<String, String> getBucketingScheme()
        {
            return bucketingScheme;
        }

        public SetMultimap<String, String> getPartitioningScheme()
        {
            return partitioningScheme;
        }

        public void checkTrinoConfiguration(CommandSpec spec, TrinoClient trinoClient)
        {
            List<String> catalogs = trinoClient.selectSingleStringColumn("SHOW CATALOGS");
            if (!catalogs.contains(requiredCatalog)) {
                throw new ExecutionException(spec.commandLine(), format("Catalog must be installed: %s", requiredCatalog));
            }
            if (this == tpch) {
                List<List<Object>> rows = trinoClient.execute("SHOW COLUMNS FROM tpch.tiny.lineitem");
                boolean standardColumnNamingEnabled = false;
                boolean decimalEnabled = false;
                for (List<Object> row : rows) {
                    String columnName = (String) row.get(0);
                    if (columnName.equals("l_quantity")) {
                        standardColumnNamingEnabled = true;
                        String columnType = (String) row.get(1);
                        decimalEnabled = columnType.contains("decimal");
                    }
                }
                if (!standardColumnNamingEnabled) {
                    throw new ExecutionException(spec.commandLine(), "tpch catalog must be configured with tpch.column-naming=STANDARD");
                }
                if (!decimalEnabled) {
                    throw new ExecutionException(spec.commandLine(), "tpch catalog must be configured with tpch.double-type-mapping=DECIMAL");
                }
            }
        }
    }
}
