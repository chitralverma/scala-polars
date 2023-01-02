package org.polars.scala.polars.internal.jni

import com.github.sbt.jni.syntax.NativeLoader

// TODO move the name of the native library elsewhere
abstract class Natively extends NativeLoader("divider") {}
