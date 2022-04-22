package io.trino.verifier.queries;

import io.airlift.log.Logging;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.io.PrintStream;

import static io.airlift.log.Level.WARN;
import static io.trino.verifier.queries.MainCommand.VersionProvider;
import static java.util.Objects.requireNonNullElse;
import static java.util.logging.Level.WARNING;

@Command(
        name = "trino-verifier-queries",
        usageHelpAutoWidth = true,
        versionProvider = VersionProvider.class,
        subcommands = {SuitesCommand.class, SchemasCommand.class})
public class MainCommand
{
    static {
        // To prevent Logging#initialize from printing an annoying "Logging to stderr" message
        java.util.logging.Logger.getLogger(Logging.class.getName()).setLevel(WARNING);

        // Save System.err and System.out
        PrintStream err = System.err;
        PrintStream out = System.out;

        // This will overwrite System.err and System.out
        Logging logging = Logging.initialize();

        // Restore System.err and System.out
        System.setErr(err);
        System.setOut(out);

        // Log only important messages to avoid cluttering the output
        logging.setRootLevel(WARN);
    }

    @CommandLine.Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    private boolean usageHelpRequested;

    @CommandLine.Option(names = "--version", versionHelp = true, description = "Print version information and exit")
    private boolean versionInfoRequested;

    public static class VersionProvider
            implements CommandLine.IVersionProvider
    {
        @CommandLine.Spec
        public CommandLine.Model.CommandSpec spec;

        @Override
        public String[] getVersion()
        {
            String version = getClass().getPackage().getImplementationVersion();
            return new String[] {spec.name() + " " + requireNonNullElse(version, "(version unknown)")};
        }
    }
}
