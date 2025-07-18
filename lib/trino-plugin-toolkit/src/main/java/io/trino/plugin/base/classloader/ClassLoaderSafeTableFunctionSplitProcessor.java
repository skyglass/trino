/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.base.classloader;

import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.function.table.TableFunctionProcessorState;
import io.trino.spi.function.table.TableFunctionSplitProcessor;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public final class ClassLoaderSafeTableFunctionSplitProcessor
        implements TableFunctionSplitProcessor
{
    private final TableFunctionSplitProcessor delegate;
    private final ClassLoader classLoader;

    public ClassLoaderSafeTableFunctionSplitProcessor(TableFunctionSplitProcessor delegate, ClassLoader classLoader)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
    }

    @Override
    public TableFunctionProcessorState process()
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            return delegate.process();
        }
    }

    @Override
    public void close()
            throws IOException
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(classLoader)) {
            delegate.close();
        }
    }
}
