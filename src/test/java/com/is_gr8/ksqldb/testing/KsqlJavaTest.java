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

    @TestFactory
    Stream<DynamicTest> testBazFailingPipeline() {
        return ksqlTestFactory.findKsqlNegativeTestCases("ksql-samples/baz");
    }

    @Test
    void testBarSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/01_example/example-from-ksql-docs.ksql",
                "ksql-samples/bar/01_example/input.json",
                "ksql-samples/bar/01_example/output.json"
        );
    }

    @Test
    void testBazSingleFileShouldFail() {
        ksqlTestFactory.runKsqlTestCaseShouldFail(
                "ksql-samples/baz/01_example/failing-example.ksql",
                "ksql-samples/baz/01_example/negativeInput.json",
                "ksql-samples/baz/01_example/negativeOutput.json"
        );
    }

    @Test
    void testBarSrcMainKsqlSamplesSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/03_example/example-from-ksql-docs.ksql",
                "ksql-samples/bar/03_example/input.json",
                "ksql-samples/bar/03_example/output.json"
        );
    }

    @Test
    void testBarSrcMainKsqlSamplesSingleFileShouldFail() {
        ksqlTestFactory.runKsqlTestCaseShouldFail(
                "ksql-samples/baz/03_example/failing-example.ksql",
                "ksql-samples/baz/03_example/negativeInput.json",
                "ksql-samples/baz/03_example/negativeOutput.json"
        );
    }

    @Test
    void testBarSrcMainPipelineSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/04_example/example-from-ksql-docs.ksql",
                "ksql-samples/bar/04_example/input.json",
                "ksql-samples/bar/04_example/output.json"
        );
    }

    @Test
    void testBazSrcMainPipelineSingleFileShouldFail() {
        ksqlTestFactory.runKsqlTestCaseShouldFail(
                "ksql-samples/baz/04_example/failing-example.ksql",
                "ksql-samples/baz/04_example/negativeInput.json",
                "ksql-samples/baz/04_example/negativeOutput.json"
        );
    }

    @Test
    void testBarSrcMainResourcesPipelineSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
                "ksql-samples/bar/05_example/example-from-ksql-docs.ksql",
                "ksql-samples/bar/05_example/input.json",
                "ksql-samples/bar/05_example/output.json"
        );
    }

    @Test
    void testBazSrcMainResourcesSingleFileShouldFail() {
        ksqlTestFactory.runKsqlTestCaseShouldFail(
                "ksql-samples/baz/05_example/failing-example.ksql",
                "ksql-samples/baz/05_example/negativeInput.json",
                "ksql-samples/baz/05_example/negativeOutput.json"
        );
    }

}
