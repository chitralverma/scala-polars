package com.github.chitralverma.polars.internal.jni

import com.github.chitralverma.polars.loadLibraryIfRequired

private[jni] trait Natively { loadLibraryIfRequired() }
