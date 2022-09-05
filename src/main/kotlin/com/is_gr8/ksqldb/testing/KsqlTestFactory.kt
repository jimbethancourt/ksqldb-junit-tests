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


class KsqlTestFactory(_ksqlFileExtension: String = "ksql") {
    private val ksqlExtension = _ksqlFileExtension
    private val objectMapper: ObjectMapper = TestJsonMapper.INSTANCE.get()

    private val ksqlSrcDir = "src/main/ksql"
    private val pipelineSrcDir = "src/main/pipeline"
    private val resourcesDir = "src/main/resources"


    @JvmOverloads
    fun findKsqlTestCases(
        pathName: String,
        inputFileName: String = "input.json",
        outputFileName: String = "output.json"
    ): Stream<DynamicTest> {
        val path = File(pathName).absolutePath
        val ksqlPath = File(ksqlSrcDir + File.separator + pathName).absolutePath
        val pipelinePath = File(pipelineSrcDir + File.separator + pathName).absolutePath
        val resourcesPath = File(resourcesDir + File.separator + pathName).absolutePath

        val dynamicTests =
            createDynamicTests(path, inputFileName, outputFileName) +
                    createDynamicTests(ksqlPath, inputFileName, outputFileName) +
                    createDynamicTests(pipelinePath, inputFileName, outputFileName) +
                    createDynamicTests(resourcesPath, inputFileName, outputFileName);

        return dynamicTests.asStream()
    }

    private fun createDynamicTests(
        ksqlFilesPath: String,
        inputFileName: String,
        outputFileName: String
    ) = File(ksqlFilesPath).walk()
        .filter { file: File ->
            file.isDirectory && file.listFiles()?.any { it.extension == ksqlExtension } == true
        }
        .map { testCaseFolder: File ->
            val contents = testCaseFolder.listFiles()!!
            val ksqlFile = contents.first { it.extension == ksqlExtension }
            val inputFile = contents.first { it.name == inputFileName }
            val outputFile = contents.first { it.name == outputFileName }
            createDynamicTestFromTriple(ksqlFile, inputFile, outputFile)
        }

    @JvmOverloads
    fun findKsqlNegativeTestCases(
        pathName: String,
        inputFileName: String = "negativeInput.json",
        outputFileName: String = "negativeOutput.json"
    ): Stream<DynamicTest> {
        val path = File(pathName).absolutePath
        val ksqlPath = File(ksqlSrcDir + File.separator + pathName).absolutePath
        val pipelinePath = File(pipelineSrcDir + File.separator + pathName).absolutePath
        val resourcesPath = File(resourcesDir + File.separator + pathName).absolutePath

        val dynamicNegativeTests =
            createDynamicNegativeTests(path, inputFileName, outputFileName) +
                    createDynamicNegativeTests(ksqlPath, inputFileName, outputFileName) +
                    createDynamicNegativeTests(pipelinePath, inputFileName, outputFileName) +
                    createDynamicNegativeTests(resourcesPath, inputFileName, outputFileName);

        return dynamicNegativeTests.asStream()
    }

    private fun createDynamicNegativeTests(
        ksqlFilesPath: String,
        inputFileName: String,
        outputFileName: String
    ) = File(ksqlFilesPath).walk()
        .filter { file: File ->
            file.isDirectory && file.listFiles()?.any { it.extension == ksqlExtension } == true
        }
        .map { testCaseFolder: File ->
            val contents = testCaseFolder.listFiles()!!
            val ksqlFile = contents.first { it.extension == ksqlExtension }
            val inputFile = contents.first { it.name == inputFileName }
            val outputFile = contents.first { it.name == outputFileName }
            createDynamicFailingTestFromTriple(ksqlFile, inputFile, outputFile)
        }

    fun runKsqlTestCase(
        ksqlFilePath: String,
        inputFilePath: String,
        outputFilePath: String
    ) {
        runKsqlTestCaseCheckDirectories(ksqlFilePath, inputFilePath, outputFilePath)
    }

    private fun runKsqlTestCaseCheckDirectories(
        ksqlFilePath: String,
        inputFilePath: String,
        outputFilePath: String
    ) {
        val path = File(ksqlFilePath).absolutePath
        val ksqlPath = File(ksqlSrcDir + File.separator + ksqlFilePath).absolutePath
        val pipelinePath = File(pipelineSrcDir + File.separator + ksqlFilePath).absolutePath
        val resourcesPath = File(resourcesDir + File.separator + ksqlFilePath).absolutePath

        if (File(path).exists()) {
            executeTestCase(
                createTestCaseFromTriple(
                    File(path),
                    File(inputFilePath),
                    File(outputFilePath)
                )
            )
        } else if (File(ksqlPath).exists()) {
            executeTestCase(
                createTestCaseFromTriple(
                    File(ksqlPath),
                    File(File(ksqlSrcDir).absolutePath + File.separator + inputFilePath),
                    File(File(ksqlSrcDir).absolutePath + File.separator + outputFilePath)
                )
            )
        } else if (File(pipelinePath).exists()) {
            executeTestCase(
                createTestCaseFromTriple(
                    File(pipelinePath),
                    File(File(pipelineSrcDir).absolutePath + File.separator + inputFilePath),
                    File(File(pipelineSrcDir).absolutePath + File.separator + outputFilePath)
                )
            )
        } else if (File(resourcesPath).exists()) {
            executeTestCase(
                createTestCaseFromTriple(
                    File(resourcesPath),
                    File(File(resourcesDir).absolutePath + File.separator + inputFilePath),
                    File(File(resourcesDir).absolutePath + File.separator + outputFilePath)
                )
            )
        }
    }

    fun runKsqlTestCaseShouldFail(
        ksqlFilePath: String,
        inputFilePath: String,
        outputFilePath: String
    ) {
        val ksqlFile = File(ksqlFilePath)
        try {
            runKsqlTestCaseCheckDirectories(ksqlFilePath, inputFilePath, outputFilePath)
            fail("Test failure expected for " + ksqlFile.path ) //line should not be reached
        } catch (e: AssertionError) {
            //do nothing, allow test to pass since failure is expected
            print("This failure is expected: \n" + e.message)
        }
    }

    private fun createDynamicTestFromTriple(ksqlFile: File, inputFile: File, outputFile: File): DynamicTest {
        val testCase = createTestCaseFromTriple(ksqlFile, inputFile, outputFile)
        return DynamicTest.dynamicTest(ksqlFile.path) { executeTestCase(testCase) }
    }

    private fun createDynamicFailingTestFromTriple(ksqlFile: File, inputFile: File, outputFile: File): DynamicTest {
        return DynamicTest.dynamicTest(ksqlFile.path) {
            try {
                executeTestCase(createTestCaseFromTriple(ksqlFile, inputFile, outputFile))
                fail("Test failure expected for " + ksqlFile.path) //line should not be reached
            } catch (e: AssertionError) {
                print("This failure is expected: \n" + e.message)
                //do nothing, allow test to pass since failure is expected
            }
        }
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
                    println(testCase.name)
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
