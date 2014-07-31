package com.facebook.presto.sql.gen.truffle;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.Page;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.truffle.TruffleFilterAndProjectCompiler.TruffleFilterAndProject;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.sql.gen.BenchmarkFilterAndProject.FILTER;
import static com.facebook.presto.sql.gen.BenchmarkFilterAndProject.PROJECT;
import static com.facebook.presto.sql.gen.BenchmarkFilterAndProject.createInputPage;
import static com.facebook.presto.sql.gen.BenchmarkFilterAndProject.execute;

public final class ExecuteTruffleFilterAndProjectCompiler
{
    // Most of the code is in BenchmarkFilterAndProject
    public static void main(String[] args)
    {
        MetadataManager metadata = new MetadataManager(new FeaturesConfig(), new TypeRegistry());
        TruffleFilterAndProject truffleFilterAndProject = TruffleFilterAndProjectCompiler.compile(metadata, FILTER, ImmutableList.of(PROJECT));

        Page inputPage = createInputPage();

        // drive the truffle method to force compilation
        while (true) {
            for (int i = 0; i < 1000; i++) {
                execute(inputPage, truffleFilterAndProject);
            }
            truffleFilterAndProject.dumpAst();
        }
    }
}
