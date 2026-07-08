package com.github.chitralverma.polars;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@code NativeLoader} — mirror of {@code NativeLoaderSuite}. Covers arch
 * normalization, classpath resource paths, and the content-addressed extraction cache.
 *
 * <p>The package-private helpers live on the Scala companion object and are reached from this
 * same-package Java test through its generated singleton.
 */
public class NativeLoaderTest {

  @Test
  public void normalizeArchMapsKnownAliases() {
    Assert.assertEquals("aarch64", NativeLoader$.MODULE$.normalizeArch("aarch64"));
    Assert.assertEquals("aarch64", NativeLoader$.MODULE$.normalizeArch("arm64"));
    Assert.assertEquals("x86_64", NativeLoader$.MODULE$.normalizeArch("amd64"));
    Assert.assertEquals("x86_64", NativeLoader$.MODULE$.normalizeArch("x86_64"));
  }

  @Test
  public void normalizeArchIsCaseInsensitive() {
    Assert.assertEquals("aarch64", NativeLoader$.MODULE$.normalizeArch("AArch64"));
    Assert.assertEquals("x86_64", NativeLoader$.MODULE$.normalizeArch("AMD64"));
  }

  @Test
  public void normalizeArchPassesThroughUnknown() {
    Assert.assertEquals("riscv64", NativeLoader$.MODULE$.normalizeArch("riscv64"));
    Assert.assertEquals("ppc64le", NativeLoader$.MODULE$.normalizeArch("PPC64LE"));
  }

  @Test
  public void resourcePathBuildsBundlePath() {
    String mapped = System.mapLibraryName("scala_polars");
    Assert.assertEquals(
        "/native/aarch64/" + mapped, NativeLoader$.MODULE$.resourcePath("scala_polars", "aarch64"));
    Assert.assertEquals(
        "/native/x86_64/" + mapped, NativeLoader$.MODULE$.resourcePath("scala_polars", "x86_64"));
  }

  @Test
  public void extractToCacheWritesExactBytes() throws IOException {
    byte[] bytes = "hello-native-java".getBytes("UTF-8");
    Path path = NativeLoader$.MODULE$.extractToCache("libtest_extract_java.bin", bytes);

    Assert.assertTrue(Files.isRegularFile(path));
    Assert.assertArrayEquals(bytes, Files.readAllBytes(path));
  }

  @Test
  public void extractToCacheIsContentAddressed() throws IOException {
    byte[] bytes = "idempotent-java".getBytes("UTF-8");
    Path first = NativeLoader$.MODULE$.extractToCache("libidem_java.bin", bytes);
    Path second = NativeLoader$.MODULE$.extractToCache("libidem_java.bin", bytes);

    Assert.assertEquals(first, second);
  }

  @Test
  public void extractToCacheSeparatesDifferingContent() throws IOException {
    Path a =
        NativeLoader$.MODULE$.extractToCache("libdiff_java.bin", "content-A".getBytes("UTF-8"));
    Path b =
        NativeLoader$.MODULE$.extractToCache("libdiff_java.bin", "content-B".getBytes("UTF-8"));

    Assert.assertNotEquals(a, b);
    Assert.assertNotEquals(a.getParent(), b.getParent());
  }

  @Test
  public void loadIsIdempotentOnceLoaded() {
    Assert.assertNotNull(Polars.version());
    NativeLoader$.MODULE$.load("scala_polars"); // must not throw
  }

  @Test
  public void loadingNonExistentLibraryThrowsDescriptively() {
    IllegalStateException ex =
        Assert.assertThrows(
            IllegalStateException.class,
            () -> NativeLoader$.MODULE$.load("scala_polars_does_not_exist"));
    Assert.assertTrue(ex.getMessage().contains("scala_polars_does_not_exist"));
  }
}
