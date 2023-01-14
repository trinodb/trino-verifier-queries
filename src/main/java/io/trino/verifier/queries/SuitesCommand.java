package io.trino.verifier.queries;

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExecutionException;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

import java.io.File;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.ScopeType.INHERIT;

@Command(name = "suites")
public class SuitesCommand
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

    private Suites suites;
    private VerifierQueryDao dao;

    @Command
    public void list()
    {
        init();
        suites.getAllIds().forEach(System.out::println);
    }

    @Command(sortOptions = false)
    public void create(
            @Option(names = "--catalog", required = true, order = 1) String catalog,
            @Option(names = "--schema", required = true, order = 2) String schema,
            @Option(names = "--name", required = true, order = 3) String name,
            @Option(names = "--overwrite", order = 4) boolean overwrite,
            @Option(names = "--suite", required = true, order = 5, description = "Suite name") String suiteName,
            @Option(names = "--control", order = 6, description = "Control name") Optional<String> control,
            @Option(names = "--create-table-properties", order = 7, defaultValue = "", description = "Additional table properties provided to CREATE TABLE statements") String createTableProperties)
    {
        init();
        Optional<Suite> suite = suites.get(suiteName, control);
        if (suite.isEmpty()) {
            throw new ExecutionException(spec.commandLine(), format("Suite not found: %s", suiteName));
        }
        if (dao.getCount(name) > 0) {
            if (!overwrite) {
                throw new ExecutionException(spec.commandLine(), format("Suite already exist: %s", name));
            }
            dao.delete(name);
        }
        dao.insert(suite.get().getQueries().stream()
                .map(queryPair -> {
                    if (createTableProperties.isEmpty()) {
                        return queryPair;
                    }
                    return queryPair.preProcessSql(sql -> sql.replace("--WITH--", "WITH (" + createTableProperties + ")"));
                })
                .map(queryPair -> new VerifierQuery(
                        name,
                        queryPair.getId(),
                        catalog,
                        schema,
                        queryPair.getTest().getSql(),
                        catalog,
                        schema,
                        queryPair.getControl().getSql()))
                .collect(toImmutableList()));
    }

    private void init()
    {
        verify(configFile != null, "configFile is null");
        System.setProperty("config", configFile.getAbsolutePath());

        loadMysqlDriver();

        ImmutableList.Builder<Module> modules = ImmutableList.<Module>builder()
                .add(new SuitesModule());

        Bootstrap app = new Bootstrap(modules.build());
        Injector injector;
        try {
            injector = app.initialize();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
        suites = injector.getInstance(Suites.class);
        dao = injector.getInstance(VerifierQueryDao.class);
    }

    private static void loadMysqlDriver()
    {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
