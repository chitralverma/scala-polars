package org.polars.scala.polars.api.io.builders;


import org.polars.scala.polars.api.LazyFrame;
import org.polars.scala.polars.internal.jni.io.scan;

public final class IPCInputBuilder extends InputBuilder<IPCInputBuilder> {

    boolean memoryMap = false;

    public final IPCInputBuilder memoryMap(boolean mMap) {
        this.memoryMap = mMap;
        return self();
    }

    @Override
    LazyFrame scanImpl(String path) {
        long ptr = scan.scanIPC(
                path,
                nRows,
                cache,
                reChunk,
                memoryMap,
                rowCountColName,
                rowCountColOffset
        );

        return LazyFrame.withPtr(ptr);
    }
}
