package org.polars.scala.polars.api.io.builders;


import org.polars.scala.polars.api.LazyFrame;
import org.polars.scala.polars.internal.jni.io.scan;

public final class CSVInputBuilder extends InputBuilder<CSVInputBuilder> {
    char delimiter = ',';

    boolean hasHeader = true;

    long inferSchemaRows = 100L;

    int skipRowsAfterHeader = 0;

    boolean ignoreErrors = false;

    boolean parseDates = false;

    boolean lowMemory = false;

    public final CSVInputBuilder delimiter(char delimiter) {
        this.delimiter = delimiter;
        return self();
    }

    public final CSVInputBuilder hasHeader(boolean hasHeader) {
        this.hasHeader = hasHeader;
        return self();
    }

    public final CSVInputBuilder inferSchemaRows(long inferSchemaRows) {
        this.inferSchemaRows = inferSchemaRows;
        return self();
    }

    public final CSVInputBuilder skipRowsAfterHeader(int skipRowsAfterHeader) {
        this.skipRowsAfterHeader = skipRowsAfterHeader;
        return self();
    }

    public final CSVInputBuilder ignoreErrors(boolean ignoreErrors) {
        this.ignoreErrors = ignoreErrors;
        return self();
    }

    public final CSVInputBuilder parseDates(boolean parseDates) {
        this.parseDates = parseDates;
        return self();
    }

    public final CSVInputBuilder lowMemory(boolean lowMemory) {
        this.lowMemory = lowMemory;
        return self();
    }

    @Override
    LazyFrame scanImpl(String path) {
        long ptr = scan.scanCSV(
                path,
                nRows,
                delimiter,
                hasHeader,
                inferSchemaRows,
                skipRowsAfterHeader,
                ignoreErrors,
                parseDates,
                cache,
                reChunk,
                lowMemory,
                rowCountColName,
                rowCountColOffset
        );


        return LazyFrame.withPtr(ptr);
    }
}
