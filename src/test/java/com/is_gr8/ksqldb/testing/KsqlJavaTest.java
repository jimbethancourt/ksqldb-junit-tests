package com.is_gr8.ksqldb.testing;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.util.stream.Stream;

class KsqlJavaTest {

    private final KsqlTestFactory ksqlTestFactory = new KsqlTestFactory();

    @TestFactory
    Stream<DynamicTest> testBarPipeline() {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar");
    }

    @TestFactory
    Stream<DynamicTest> testFooPipeline() {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/foo");
    }
}
