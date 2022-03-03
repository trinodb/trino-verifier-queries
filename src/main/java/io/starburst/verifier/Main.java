package io.starburst.verifier;

import picocli.CommandLine;

import static java.lang.System.exit;

public final class Main
{
    private Main() {}

    public static void main(String[] args)
    {
        exit(new CommandLine(new MainCommand()).execute(args));
    }
}
