package org.polars.scala.polars.internal.jni

import org.polars.scala.polars.loadLibraryIfRequired

private[jni] trait Natively { loadLibraryIfRequired() }
