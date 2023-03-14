package examples.java.configuration;

import org.polars.scala.polars.Polars;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;

public class ConfiguringPolars {

    public static void main(String[] args) throws IOException {

        /* Checking the version scala-polars is compiled against. */
        String version = Polars.version();
        System.out.printf("scala-polars has been compiled against version '%s'%n%n", version);

        /* Get default configuration. */
        System.out.printf("Default Configuration:%n%s%n%n", Polars.config());

        /* Updating configuration. */

        /* Update the number of rows shown while doing `df.show()` */
        Polars.config().update().withMaxTableRows(20).apply();
        System.out.printf("After updating number of rows:%n%s%n%n", Polars.config());

        /* Update the number of columns shown while doing `df.show()` */
        Polars.config().update().withMaxTableColumns(20).apply();
        System.out.printf("After updating number of columns:%n%s%n%n", Polars.config());

        /* Reset config */
        Polars.config().update().reset().apply();
        System.out.printf("After resetting config:%n%s%n%n", Polars.config());

        /* Chaining configuration options */
        HashMap<String, String> options = new HashMap<>();
        options.put("POLARS_TABLE_WIDTH", "5000");

        Polars.config()
                .update()
                .withMaxTableRows(20)
                .withMaxTableColumns(20)
                .withOption("POLARS_FMT_TABLE_CELL_ALIGNMENT", "RIGHT")
                .withOptions(options)
                .apply();

        System.out.printf("After chained configs:%n%s%n%n", Polars.config());

        /* Persisting current configuration to file */
        Path tempDirectory = Files.createTempDirectory("polars-config-");
        File tempFile = Files.createTempFile(tempDirectory, "temp-polars-config-", "plcfg").toFile();
        Polars.config().saveTo(tempFile, true);

        /* Reloading current configuration to file */
        Polars.config().update().reset().apply();
        System.out.printf("After resetting config:%n%s%n%n", Polars.config());

        Polars.config().update().fromPath(tempFile).apply();
        System.out.printf("After reloading config from file path:%n%s%n", Polars.config());

    }
}
