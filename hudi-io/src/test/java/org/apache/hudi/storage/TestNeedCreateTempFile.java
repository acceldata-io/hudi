/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.storage;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Answers;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * Tests which schemes publish immutable files through a temp file and rename.
 */
public class TestNeedCreateTempFile {

  @ParameterizedTest
  @ValueSource(strings = {"hdfs", "viewfs", "file", "gvfs"})
  public void testSchemePublishesImmutableFileViaTempFile(String scheme) {
    assertTrue(storageWithScheme(scheme).needCreateTempFile());
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3", "s3a", "gs"})
  public void testObjectStoreWritesImmutableFileInPlace(String scheme) {
    assertFalse(storageWithScheme(scheme).needCreateTempFile());
  }

  @ParameterizedTest
  @ValueSource(strings = {"hdfs", "gvfs"})
  public void testPartiallyWrittenTimelineFileIsNotCreatedAtTheFinalName(String scheme) throws Exception {
    HoodieStorage storage = storageWithScheme(scheme);
    StoragePath target = new StoragePath(scheme + "://namenode:8020/table/.hoodie/20240101000000.commit");
    byte[] content = "commit-metadata".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream written = new ByteArrayOutputStream();
    AtomicReference<StoragePath> createdPath = new AtomicReference<>();

    doAnswer(captureCreatedPath(written, createdPath, target)).when(storage)
        .create(any(StoragePath.class), eq(false));
    doAnswer(invocation -> {
      assertEquals(createdPath.get(), invocation.getArgument(0));
      assertEquals(target, invocation.getArgument(1));
      return true;
    }).when(storage).rename(any(StoragePath.class), eq(target));

    storage.createImmutableFileInPath(target, Option.of(HoodieInstantWriter.convertByteArrayToWriter(content)));

    StoragePath tempPath = createdPath.get();
    assertNotEquals(target, tempPath);
    assertEquals(target.getParent(), tempPath.getParent());
    assertTrue(tempPath.getName().startsWith(target.getName() + "."));
    assertArrayEquals(content, written.toByteArray());
    verify(storage, never()).create(eq(target), anyBoolean());
    verify(storage).rename(tempPath, target);
  }

  @ParameterizedTest
  @ValueSource(strings = {"s3a", "gs"})
  public void testObjectStoreCreatesTheTimelineNameDirectly(String scheme) throws Exception {
    HoodieStorage storage = storageWithScheme(scheme);
    StoragePath target = new StoragePath(scheme + "://bucket/table/.hoodie/20240101000000.commit");
    byte[] content = "commit-metadata".getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream written = new ByteArrayOutputStream();

    doAnswer(invocation -> {
      assertEquals(target, invocation.getArgument(0));
      return recordingStream(written);
    }).when(storage).create(eq(target), eq(false));

    storage.createImmutableFileInPath(target, Option.of(HoodieInstantWriter.convertByteArrayToWriter(content)));

    assertArrayEquals(content, written.toByteArray());
    verify(storage, never()).rename(any(StoragePath.class), any(StoragePath.class));
  }

  private static HoodieStorage storageWithScheme(String scheme) {
    HoodieStorage storage = mock(HoodieStorage.class, withSettings().defaultAnswer(Answers.CALLS_REAL_METHODS));
    doAnswer(invocation -> scheme).when(storage).getScheme();
    return storage;
  }

  private static Answer<OutputStream> captureCreatedPath(ByteArrayOutputStream written,
                                                          AtomicReference<StoragePath> createdPath,
                                                          StoragePath target) {
    return invocation -> {
      StoragePath path = invocation.getArgument(0);
      assertNotEquals(target, path);
      createdPath.set(path);
      return recordingStream(written);
    };
  }

  private static OutputStream recordingStream(ByteArrayOutputStream written) {
    return new OutputStream() {
      @Override
      public void write(int b) {
        written.write(b);
      }
    };
  }
}
