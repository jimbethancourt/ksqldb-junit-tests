package com.is_gr8.ksqldb.testing

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.api.extension.ExtendWith
import java.util.*
import java.util.stream.Stream

@ExtendWith(KsqlParameterResolverExtension::class)
class TestExtensionTest(private val ksqlTestFactory: KsqlTestFactory) {

    @TestFactory
    internal fun testBarPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar")
    }

    /*
    //Fails when uncommented.  This is expcted, but not sure how to assert AssertionError expected for Dynamic Tests
    @TestFactory
    internal fun testBazPipeline(): Stream<DynamicTest> {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/baz") // {AssertionError::class.java }
    }*/

}
