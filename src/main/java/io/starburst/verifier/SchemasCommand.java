package io.starburst.verifier;

import com.google.common.collect.ImmutableList;
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

    @Command(sortOptions = false)
    public void create(
            @Option(names = "--catalog", required = true, order = 1) String catalog,
            @Option(names = "--schema", required = true, order = 2) String schema,
            @Option(names = "--overwrite", order = 3) boolean overwrite,
            @Option(names = "--bucket-count", order = 4, defaultValue = "0") int bucketCount,
            @Option(names = "--scale-factor", order = 5, defaultValue = "0.1") BigDecimal scaleFactor,
            @Option(names = "--threads", order = 6, defaultValue = "1") int threads,
            @Option(names = "--schema-type", required = true, order = 7, description = "Schema type. Valid values: ${COMPLETION-CANDIDATES}") SchemaType schemaType)
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

    private void copyTable(String catalogFrom, String schemaFrom, String catalogTo, String schemaTo, String tableName, Set<String> bucketingScheme, int bucketCount)
    {
        String bucketingColumns = bucketingScheme.stream().map(value -> "'" + value + "'").collect(joining(" , "));
        String sql = format("CREATE TABLE \"%s\".\"%s\".\"%s\" ", catalogTo, schemaTo, tableName);
        if (!bucketingColumns.isEmpty()) {
            sql += format("WITH (bucketed_by = ARRAY[%s], bucket_count = %s) ", bucketingColumns, bucketCount);
        }
        sql += format("AS SELECT * FROM \"%s\".\"%s\".\"%s\"", catalogFrom, schemaFrom, tableName);
        System.err.println(format("Creating table %s.%s.%s based on %s.%s.%s", catalogTo, schemaTo, tableName, catalogFrom, schemaFrom, tableName) +
                (bucketingColumns.isEmpty() ? "" : format(" bucketed on %s", bucketingColumns)));
        try {
            trinoClient.executeUpdate(sql);
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
                        .build()),
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
                ImmutableSetMultimap.<String, String>builder()
                        // TODO
                        .build()),
        /**/;

        private final String requiredCatalog;
        private final List<String> tables;
        private final SetMultimap<String, String> bucketingScheme;

        SchemaType(String requiredCatalog, List<String> tables, SetMultimap<String, String> bucketingScheme)
        {
            this.requiredCatalog = requireNonNull(requiredCatalog, "requiredCatalog is null");
            this.tables = List.copyOf(requireNonNull(tables, "tables is null"));
            this.bucketingScheme = ImmutableSetMultimap.copyOf(requireNonNull(bucketingScheme, "bucketingScheme is null"));
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
