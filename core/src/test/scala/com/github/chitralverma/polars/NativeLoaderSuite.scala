package com.github.chitralverma.polars

import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Unit tests for `NativeLoader` — arch normalization, classpath resource paths, and the
  * content-addressed extraction cache. The Java mirror is `NativeLoaderTest`.
  */
class NativeLoaderSuite extends AnyFunSuite with Matchers {

  test("normalizeArch maps known aliases to the bundle directory names") {
    NativeLoader.normalizeArch("aarch64") shouldBe "aarch64"
    NativeLoader.normalizeArch("arm64") shouldBe "aarch64"
    NativeLoader.normalizeArch("amd64") shouldBe "x86_64"
    NativeLoader.normalizeArch("x86_64") shouldBe "x86_64"
  }

  test("normalizeArch is case-insensitive") {
    NativeLoader.normalizeArch("AArch64") shouldBe "aarch64"
    NativeLoader.normalizeArch("ARM64") shouldBe "aarch64"
    NativeLoader.normalizeArch("AMD64") shouldBe "x86_64"
  }

  test("normalizeArch passes through unknown architectures (lowercased)") {
    NativeLoader.normalizeArch("riscv64") shouldBe "riscv64"
    NativeLoader.normalizeArch("PPC64LE") shouldBe "ppc64le"
  }

  test("resourcePath builds the bundled /native/<arch>/ path with the mapped library name") {
    val mapped = System.mapLibraryName("scala_polars")
    NativeLoader.resourcePath("scala_polars", "aarch64") shouldBe s"/native/aarch64/$mapped"
    NativeLoader.resourcePath("scala_polars", "x86_64") shouldBe s"/native/x86_64/$mapped"
  }

  test("extractToCache writes the exact bytes to a cache file") {
    val bytes = "hello-native".getBytes("UTF-8")
    val path = NativeLoader.extractToCache("libtest_extract.bin", bytes)

    Files.isRegularFile(path) shouldBe true
    Files.readAllBytes(path) shouldBe bytes
    path.getFileName.toString shouldBe "libtest_extract.bin"
  }

  test("extractToCache is content-addressed: identical content reuses the same path") {
    val bytes = "idempotent-content".getBytes("UTF-8")
    val first = NativeLoader.extractToCache("libidem.bin", bytes)
    val second = NativeLoader.extractToCache("libidem.bin", bytes)

    second shouldBe first
  }

  test("extractToCache puts differing content under different (hashed) directories") {
    val a = NativeLoader.extractToCache("libdiff.bin", "content-A".getBytes("UTF-8"))
    val b = NativeLoader.extractToCache("libdiff.bin", "content-B".getBytes("UTF-8"))

    a should not be b
    a.getParent should not be b.getParent
  }

  test("load is idempotent once the library is already loaded") {
    // The test harness loads the library before this runs; loading again must be a no-op.
    Polars.version() should not be null
    noException should be thrownBy NativeLoader.load("scala_polars")
  }

  test("loading a non-existent library surfaces a descriptive IllegalStateException") {
    val ex = intercept[IllegalStateException] {
      NativeLoader.load("scala_polars_does_not_exist")
    }
    ex.getMessage should include("scala_polars_does_not_exist")
  }
}
