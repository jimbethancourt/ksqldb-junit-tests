package org.hjug.ksqldb.testing

import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory
import java.util.stream.Stream

class KsqlTest {

    @TestFactory
    fun testBarPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar")
    }

    @TestFactory
    fun testFooPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/foo")
    }

    @TestFactory
    fun testBazFailingPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlNegativeTestCases("ksql-samples/baz")
    }

    @Test
    fun testBarPipelineSingleFile() {
        ksqlTestFactory.runKsqlTestCase(
            "ksql-samples/bar/01_example/example-from-ksql-docs.ksql",
            "ksql-samples/bar/01_example/input.json",
            "ksql-samples/bar/01_example/output.json"
        )
    }

    @Test
    fun testBazPipelineSingleFileShouldFail() {
        return ksqlTestFactory.runKsqlTestCaseShouldFail(
            "ksql-samples/baz/01_example/failing-example.ksql",
            "ksql-samples/baz/01_example/negativeInput.json",
            "ksql-samples/baz/01_example/negativeOutput.json"
        )
    }

    companion object {
        private val ksqlTestFactory = KsqlTestFactory()
    }

}



