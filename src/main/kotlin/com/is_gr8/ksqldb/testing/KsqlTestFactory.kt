package com.is_gr8.ksqldb.testing

import com.fasterxml.jackson.databind.ObjectMapper
import io.confluent.ksql.parser.DefaultKsqlParser
import io.confluent.ksql.test.model.*
import io.confluent.ksql.test.tools.*
import io.confluent.ksql.util.KsqlException
import io.confluent.ksql.util.PersistentQueryMetadata
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.fail
import java.io.File
import java.io.IOException
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
import java.util.stream.Stream
import kotlin.streams.asStream

class KsqlTestFactory (_ksqlFileExtension: String = "ksql") {
    private val ksqlExtension = _ksqlFileExtension
    private val objectMapper: ObjectMapper = TestJsonMapper.INSTANCE.get()


    @JvmOverloads
    fun findKsqlTestCases(
        pathName: String,
        inputFileName: String = "input.json",
        outputFileName: String = "output.json"
    ): Stream<DynamicTest> {
        return File(pathName).walk()
            .filter { file: File -> file.isDirectory && file.listFiles()?.any { it.extension == ksqlExtension } == true}
            .map { testCaseFolder: File ->
                val contents = testCaseFolder.listFiles()!!
                val ksqlFile = contents.first { it.extension == ksqlExtension }
                val inputFile = contents.first { it.name == inputFileName }
                val outputFile = contents.first { it.name == outputFileName }
                createDynamicTestFromTriple(ksqlFile, inputFile, outputFile)
            }.asStream()
    }

    fun runKsqlTestCase(
        ksqlFilePath: String,
        inputFilePath: String,
        outputFilePath: String
    ) {
        val ksqlFile = File(ksqlFilePath)
        val inputFile = File(inputFilePath)
        val outputFile = File(outputFilePath)
        executeTestCase(createTestCaseFromTriple(ksqlFile, inputFile, outputFile))
    }

    fun runKsqlTestCaseShouldFail(
        ksqlFilePath: String,
        inputFilePath: String,
        outputFilePath: String
    ) {
        val ksqlFile = File(ksqlFilePath)
        val inputFile = File(inputFilePath)
        val outputFile = File(outputFilePath)
        try {
            executeTestCase(createTestCaseFromTriple(ksqlFile, inputFile, outputFile))
            fail("Test failure expected") //line should not be reached
        } catch (e: AssertionError) {
            //do nothing, allow test to pass since failure is expected
        }
    }

    private fun createDynamicTestFromTriple(ksqlFile: File, inputFile: File, outputFile: File): DynamicTest {
        val testCase = createTestCaseFromTriple(ksqlFile, inputFile, outputFile)
        return DynamicTest.dynamicTest(ksqlFile.path) { executeTestCase(testCase) }
    }

    private fun createTestCaseFromTriple(
        ksqlFile: File,
        inputFile: File,
        outputFile: File
    ): TestCase {
        val statements = getKsqlStatements(ksqlFile)
        val inputRecordNodes: InputRecordsNode? = readInputRecords(inputFile)
        val outRecordNodes: OutputRecordsNode = readOutputRecordNodes(outputFile)

        val testCaseNode = TestCaseNode(
            "KSQL_Test",
            Optional.empty(),
            null,
            emptyList<String>(),
            inputRecordNodes?.inputRecords,
            outRecordNodes.outputRecords,
            emptyList(),
            statements,
            null,
            null as ExpectedExceptionNode?,
            null as PostConditionsNode?,
            true
        )
        val stmtsPath: Path = Paths.get(ksqlFile.absolutePath)
        val location = PathLocation(stmtsPath)

        return TestCaseBuilder.buildTests(testCaseNode, ksqlFile.toPath()) { location }[0]
    }

    private fun readOutputRecordNodes(outputFile: File): OutputRecordsNode {
        return try {
            objectMapper.readValue(
                outputFile,
                OutputRecordsNode::class.java
            )
        } catch (var8: Exception) {
            throw RuntimeException("File name: " + outputFile + " Message: " + var8.message)
        }
    }

    private fun readInputRecords(inputFile: File): InputRecordsNode? {
        return try {
            if (inputFile.exists()) objectMapper.readValue(
                inputFile,
                InputRecordsNode::class.java
            ) else null
        } catch (e: Exception) {
            throw RuntimeException(String.format("File name: %s Message: %s", inputFile, e.message))
        }
    }

    private fun executeTestCase(testCase: TestCase) {
        val testExecutor = TestExecutor.create(true, Optional.empty())
        try {
            val testExecutionListener = object : TestExecutionListener {
                override fun acceptQuery(query: PersistentQueryMetadata?) {
                    // a printed topology might be helpful when tests fail
                    print(query?.topologyDescription)
                }
            }
            testExecutor.buildAndExecuteQuery(testCase, testExecutionListener)
            //allow AssertionError to bubble up to allow for creation of negative tests
        } catch (e: Exception) {
            fail(e.message)
        } finally {
            testExecutor.close()
        }
    }

    private fun getKsqlStatements(queryFile: File): List<String> {
        return try {
            val sqlStatements = queryFile.readText()
            DefaultKsqlParser().let { parser ->
                parser.parse(sqlStatements).map { it.statementText }
            }
        } catch (e: IOException) {
            throw KsqlException("Could not read the query file: $queryFile. Details: ${e.message}", e)
        }
    }
}
