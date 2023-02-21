package org.polars.scala

import java.util.concurrent.atomic.AtomicReference

import scala.util.{Failure, Success, Try}

import org.json4s.DefaultFormats

package object polars {

  private final val NATIVE_LIB_NAME = "scala_polars"

  private[polars] val libraryLoaded =
    new AtomicReference[LibraryStates.LibraryState](LibraryStates.NOT_LOADED)

  implicit val formats: DefaultFormats.type = DefaultFormats

  private[polars] def loadLibraryIfRequired(): Unit = {
    if (libraryLoaded.get() == LibraryStates.LOADED)
      return

    if (libraryLoaded.compareAndSet(LibraryStates.NOT_LOADED, LibraryStates.LOADING)) {
      Try(NativeLoader.load(NATIVE_LIB_NAME)) match {
        case Success(_) =>
          libraryLoaded.set(LibraryStates.LOADED)

        case Failure(e) =>
          libraryLoaded.set(LibraryStates.NOT_LOADED)
          throw new RuntimeException(s"Unable to load the `$NATIVE_LIB_NAME` native library.", e)
      }

      return
    }

    while (libraryLoaded.get() == LibraryStates.LOADING)
      Thread.sleep(10)
  }

}
