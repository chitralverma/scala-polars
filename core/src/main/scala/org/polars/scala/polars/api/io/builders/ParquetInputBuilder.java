package org.polars.scala.polars.api.io.builders;


import org.polars.scala.polars.api.LazyFrame;
import org.polars.scala.polars.internal.jni.io.scan;

public final class ParquetInputBuilder extends InputBuilder<ParquetInputBuilder> {

    boolean lowMemory = false;

    public final ParquetInputBuilder lowMemory(boolean lowMemory) {
        this.lowMemory = lowMemory;
        return self();
    }

    boolean hivePartitioning = false;

    public final ParquetInputBuilder hivePartitioning(boolean hivePartitioning) {
        this.hivePartitioning = hivePartitioning;
        return self();
    }

    @Override
    LazyFrame scanImpl(String path) {
        long ptr = scan.scanParquet(
                path,
                nRows,
                cache,
                reChunk,
                lowMemory,
                hivePartitioning,
                rowCountColName,
                rowCountColOffset
        );

        return LazyFrame.withPtr(ptr);
    }
}
