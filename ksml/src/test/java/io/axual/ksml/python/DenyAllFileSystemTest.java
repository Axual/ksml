package io.axual.ksml.python;

/*-
 * ========================LICENSE_START=================================
 * KSML
 * %%
 * Copyright (C) 2021 - 2026 Axual B.V.
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * =========================LICENSE_END==================================
 */

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.net.URI;
import java.nio.file.AccessMode;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DenyAllFileSystemTest {

    private static final DenyAllFileSystem FILE_SYSTEM = new DenyAllFileSystem();
    private static final Path FILE = Path.of("/etc/passwd");
    private static final Path OTHER_FILE = Path.of("/tmp/copy");

    @Test
    void answersCurrentFolderWithRoot() {
        assertThat(FILE_SYSTEM.toAbsolutePath(Path.of(""))).isEqualTo(Path.of("/"));
    }

    @Test
    void setCurrentFolderChangesNothing() {
        final var fileSystem = new DenyAllFileSystem();
        fileSystem.setCurrentWorkingDirectory(Path.of("/tmp"));
        assertThat(fileSystem.toAbsolutePath(Path.of(""))).isEqualTo(Path.of("/"));
    }

    @Test
    void parsesPaths() {
        assertThat(FILE_SYSTEM.parsePath("/etc/passwd")).isEqualTo(FILE);
        assertThat(FILE_SYSTEM.parsePath(URI.create("file:///etc/passwd"))).isEqualTo(FILE);
    }

    static Stream<Arguments> fileOperations() {
        return Stream.of(
                operation("toAbsolutePath of a relative file", () -> FILE_SYSTEM.toAbsolutePath(Path.of("etc/passwd"))),
                operation("toAbsolutePath of an absolute file", () -> FILE_SYSTEM.toAbsolutePath(FILE)),
                operation("checkAccess", () -> FILE_SYSTEM.checkAccess(FILE, Set.of(AccessMode.READ))),
                operation("createDirectory", () -> FILE_SYSTEM.createDirectory(OTHER_FILE)),
                operation("delete", () -> FILE_SYSTEM.delete(FILE)),
                operation("newByteChannel", () -> FILE_SYSTEM.newByteChannel(FILE, Set.of(StandardOpenOption.READ))),
                operation("newDirectoryStream", () -> FILE_SYSTEM.newDirectoryStream(Path.of("/etc"), path -> true)),
                operation("toRealPath", () -> FILE_SYSTEM.toRealPath(FILE)),
                operation("readAttributes", () -> FILE_SYSTEM.readAttributes(FILE, "*")),
                operation("setAttribute", () -> FILE_SYSTEM.setAttribute(FILE, "lastModifiedTime", null)),
                operation("copy", () -> FILE_SYSTEM.copy(FILE, OTHER_FILE)),
                operation("move", () -> FILE_SYSTEM.move(FILE, OTHER_FILE)),
                operation("createLink", () -> FILE_SYSTEM.createLink(OTHER_FILE, FILE)),
                operation("createSymbolicLink", () -> FILE_SYSTEM.createSymbolicLink(OTHER_FILE, FILE)),
                operation("readSymbolicLink", () -> FILE_SYSTEM.readSymbolicLink(FILE)),
                operation("getTempDirectory", FILE_SYSTEM::getTempDirectory),
                operation("isSameFile", () -> FILE_SYSTEM.isSameFile(FILE, OTHER_FILE)),
                operation("getFileStoreTotalSpace", () -> FILE_SYSTEM.getFileStoreTotalSpace(FILE)),
                operation("getFileStoreUnallocatedSpace", () -> FILE_SYSTEM.getFileStoreUnallocatedSpace(FILE)),
                operation("getFileStoreUsableSpace", () -> FILE_SYSTEM.getFileStoreUsableSpace(FILE)),
                operation("getFileStoreBlockSize", () -> FILE_SYSTEM.getFileStoreBlockSize(FILE)),
                operation("isFileStoreReadOnly", () -> FILE_SYSTEM.isFileStoreReadOnly(FILE)));
    }

    private static Arguments operation(String name, ThrowingCallable call) {
        return Arguments.of(name, call);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("fileOperations")
    void deniesEveryFileOperation(String name, ThrowingCallable call) {
        assertThatThrownBy(call)
                .isInstanceOf(SecurityException.class)
                .hasMessageStartingWith("Operation is not allowed");
    }
}
