package org.polars.scala.polars.api.io.builders;


import org.polars.scala.polars.api.LazyFrame;
import org.polars.scala.polars.internal.jni.io.scan;

public final class NdJsonInputBuilder extends InputBuilder<NdJsonInputBuilder> {

    long inferSchemaRows = 100L;

    boolean lowMemory = false;

    public final NdJsonInputBuilder lowMemory(boolean lowMemory) {
        this.lowMemory = lowMemory;
        return self();
    }

    public final NdJsonInputBuilder inferSchemaRows(long inferSchemaRows) {
        this.inferSchemaRows = inferSchemaRows;
        return self();
    }

    @Override
    LazyFrame scanImpl(String path) {
        long ptr = scan.scanNdJson(
                path,
                nRows,
                inferSchemaRows,
                cache,
                reChunk,
                lowMemory,
                rowCountColName,
                rowCountColOffset
        );

        return LazyFrame.withPtr(ptr);
    }
}
