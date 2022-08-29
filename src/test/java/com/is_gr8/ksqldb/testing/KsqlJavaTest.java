package com.is_gr8.ksqldb.testing;

import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
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

    @Test
    void testBarPipelineSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/01_example/example-from-ksql-docs.ksql");
    }

    @Test
    void testBarPipelineSingleFileSpecifyInputOutputFiles() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/01_example/example-from-ksql-docs.ksql",
                "input.json", "output.json");
    }

    @Test
    void testBazPipelineSingleFileShouldFail() {
        ksqlTestFactory.runKsqlTestCaseShouldFail(
                "ksql-samples/baz/01_example/failing-example.ksql");
    }

}
