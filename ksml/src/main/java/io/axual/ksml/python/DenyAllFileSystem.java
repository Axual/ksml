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

import org.graalvm.polyglot.io.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Map;
import java.util.Set;

/**
 * A file system that denies every operation, like {@link FileSystem#newDenyIOFileSystem()}, with one
 * exception: it answers "what is the current folder" with {@code /}.
 * <p>
 * GraalPy 25.1 and later ask for the current folder while they import {@code sysconfig}, and
 * {@code ctypes} imports {@code sysconfig}. The plain deny file system refuses that question, so
 * {@code ctypes} cannot load even when native access is allowed. The answer {@code /} is made up: it
 * gives no access to any real file, because every other operation is still denied.
 */
final class DenyAllFileSystem implements FileSystem {

    private static final Path CURRENT_FOLDER = Path.of("/");
    private final FileSystem deny = FileSystem.newDenyIOFileSystem();

    // Same exception and message as Truffle's own deny file system.
    private static SecurityException denied(Path path) {
        return new SecurityException(path == null ? "Operation is not allowed." : "Operation is not allowed for: " + path);
    }

    @Override
    public Path toAbsolutePath(Path path) {
        // Truffle asks for the current folder as the absolute form of the empty path.
        if (!path.isAbsolute() && path.toString().isEmpty()) {
            return CURRENT_FOLDER;
        }
        throw denied(path);
    }

    @Override
    public Path parsePath(URI uri) {
        return deny.parsePath(uri);
    }

    @Override
    public Path parsePath(String path) {
        return deny.parsePath(path);
    }

    @Override
    public void checkAccess(Path path, Set<? extends AccessMode> modes, LinkOption... linkOptions) throws IOException {
        throw denied(path);
    }

    @Override
    public void createDirectory(Path dir, FileAttribute<?>... attrs) throws IOException {
        throw denied(dir);
    }

    @Override
    public void delete(Path path) throws IOException {
        throw denied(path);
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
        throw denied(path);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(Path dir, DirectoryStream.Filter<? super Path> filter) throws IOException {
        throw denied(dir);
    }

    @Override
    public Path toRealPath(Path path, LinkOption... linkOptions) throws IOException {
        throw denied(path);
    }

    @Override
    public Map<String, Object> readAttributes(Path path, String attributes, LinkOption... options) throws IOException {
        throw denied(path);
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) throws IOException {
        throw denied(path);
    }

    @Override
    public void copy(Path source, Path target, CopyOption... options) throws IOException {
        throw denied(source);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        throw denied(source);
    }

    @Override
    public void createLink(Path link, Path existing) throws IOException {
        throw denied(link);
    }

    @Override
    public void createSymbolicLink(Path link, Path target, FileAttribute<?>... attrs) throws IOException {
        throw denied(link);
    }

    @Override
    public Path readSymbolicLink(Path link) throws IOException {
        throw denied(link);
    }

    @Override
    public Path getTempDirectory() {
        throw denied(null);
    }

    @Override
    public boolean isSameFile(Path path1, Path path2, LinkOption... options) throws IOException {
        throw denied(path1);
    }

    @Override
    public void setCurrentWorkingDirectory(Path currentWorkingDirectory) {
        deny.setCurrentWorkingDirectory(currentWorkingDirectory);
    }

    @Override
    public long getFileStoreTotalSpace(Path path) throws IOException {
        throw denied(path);
    }

    @Override
    public long getFileStoreUnallocatedSpace(Path path) throws IOException {
        throw denied(path);
    }

    @Override
    public long getFileStoreUsableSpace(Path path) throws IOException {
        throw denied(path);
    }

    @Override
    public long getFileStoreBlockSize(Path path) throws IOException {
        throw denied(path);
    }

    @Override
    public boolean isFileStoreReadOnly(Path path) throws IOException {
        throw denied(path);
    }
}
