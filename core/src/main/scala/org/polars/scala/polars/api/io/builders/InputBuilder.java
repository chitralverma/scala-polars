package org.polars.scala.polars.api.io.builders;

import org.polars.scala.polars.Polars;
import org.polars.scala.polars.api.DataFrame;
import org.polars.scala.polars.api.LazyFrame;
import scala.jdk.CollectionConverters;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public abstract class InputBuilder<B extends InputBuilder<B>> {

    long nRows = -1L;

    boolean cache = true;

    boolean reChunk = false;

    String rowCountColName = null;

    int rowCountColOffset = 0;

    public final B nRows(long nRows) {
        this.nRows = nRows;
        return self();
    }

    public final B cache(boolean cache) {
        this.cache = cache;
        return self();
    }

    public final B reChunk(boolean reChunk) {
        this.reChunk = reChunk;
        return self();
    }

    public final B rowCountColName(String rowCountColName) {
        this.rowCountColName = rowCountColName;
        return self();
    }

    public final B rowCountColOffset(int rowCountColOffset) {
        this.rowCountColOffset = rowCountColOffset;
        return self();
    }

    public final LazyFrame scan(String path, String... paths) {
        LazyFrame ldf = scanImpl(path);
        List<LazyFrame> lazyFrames = Arrays.stream(paths).parallel().map(this::scanImpl).collect(Collectors.toList());

        return Polars.concat(ldf,
                CollectionConverters.IterableHasAsScala(lazyFrames).asScala().toSeq(),
                reChunk,
                true);
    }

    public final DataFrame read(String path, String... paths) {
        return scan(path, paths).collect();
    }

    abstract LazyFrame scanImpl(String path);

    @SuppressWarnings("unchecked")
    final B self() {
        return (B) this;
    }

}
