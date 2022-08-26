package com.is_gr8.ksqldb.testing

import org.junit.jupiter.api.*
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

    /*
    //Fails when uncommented.  This is expcted, but not sure how to assert AssertionError expected for Dynamic Tests
    @TestFactory
    fun testBazPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/baz") // {AssertionError::class.java }
    }*/

    companion object {
        private val ksqlTestFactory = KsqlTestFactory()
    }

}



