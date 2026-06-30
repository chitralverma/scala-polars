package com.github.chitralverma.polars;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link NativeLoader} — mirror of {@code NativeLoaderSuite}. Covers arch
 * normalization, classpath resource paths, and the content-addressed extraction cache. Not a port
 * of an upstream pytest.
 *
 * <p>The package-private helpers live on the Scala companion object, so they are reached from Java
 * via {@code NativeLoader$.MODULE$}.
 */
public class NativeLoaderTest {

  private static final NativeLoader$ LOADER = NativeLoader$.MODULE$;

  @Test
  public void normalizeArchMapsKnownAliases() {
    Assert.assertEquals("aarch64", LOADER.normalizeArch("aarch64"));
    Assert.assertEquals("aarch64", LOADER.normalizeArch("arm64"));
    Assert.assertEquals("x86_64", LOADER.normalizeArch("amd64"));
    Assert.assertEquals("x86_64", LOADER.normalizeArch("x86_64"));
  }

  @Test
  public void normalizeArchIsCaseInsensitive() {
    Assert.assertEquals("aarch64", LOADER.normalizeArch("AArch64"));
    Assert.assertEquals("x86_64", LOADER.normalizeArch("AMD64"));
  }

  @Test
  public void normalizeArchPassesThroughUnknown() {
    Assert.assertEquals("riscv64", LOADER.normalizeArch("riscv64"));
    Assert.assertEquals("ppc64le", LOADER.normalizeArch("PPC64LE"));
  }

  @Test
  public void resourcePathBuildsBundlePath() {
    String mapped = System.mapLibraryName("scala_polars");
    Assert.assertEquals(
        "/native/aarch64/" + mapped, LOADER.resourcePath("scala_polars", "aarch64"));
    Assert.assertEquals("/native/x86_64/" + mapped, LOADER.resourcePath("scala_polars", "x86_64"));
  }

  @Test
  public void extractToCacheWritesExactBytes() throws IOException {
    byte[] bytes = "hello-native-java".getBytes("UTF-8");
    Path path = LOADER.extractToCache("libtest_extract_java.bin", bytes);

    Assert.assertTrue(Files.isRegularFile(path));
    Assert.assertArrayEquals(bytes, Files.readAllBytes(path));
  }

  @Test
  public void extractToCacheIsContentAddressed() throws IOException {
    byte[] bytes = "idempotent-java".getBytes("UTF-8");
    Path first = LOADER.extractToCache("libidem_java.bin", bytes);
    Path second = LOADER.extractToCache("libidem_java.bin", bytes);

    Assert.assertEquals(first, second);
  }

  @Test
  public void extractToCacheSeparatesDifferingContent() throws IOException {
    Path a = LOADER.extractToCache("libdiff_java.bin", "content-A".getBytes("UTF-8"));
    Path b = LOADER.extractToCache("libdiff_java.bin", "content-B".getBytes("UTF-8"));

    Assert.assertNotEquals(a, b);
    Assert.assertNotEquals(a.getParent(), b.getParent());
  }

  @Test
  public void loadIsIdempotentOnceLoaded() {
    Assert.assertNotNull(Polars.version());
    LOADER.load("scala_polars"); // must not throw
  }

  @Test
  public void loadingNonExistentLibraryThrowsDescriptively() {
    IllegalStateException ex =
        Assert.assertThrows(
            IllegalStateException.class, () -> LOADER.load("scala_polars_does_not_exist"));
    Assert.assertTrue(ex.getMessage().contains("scala_polars_does_not_exist"));
  }
}
