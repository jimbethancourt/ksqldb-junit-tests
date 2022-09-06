# KSQL testing with JUnit 5
This is a library that leverages JUnit 5 and can be used as an alternative to the ksql-test-runner.  
Internally it uses the same classes that the ksql-test-runner uses.

See https://docs.ksqldb.io/en/latest/how-to-guides/test-an-app/

## Why would you do this?
- better reporting in CI systems which parse junit results
- faster than the testing tool when running multiple tests
- easy to extend with custom checks, e.g. to ensure naming patterns, max number of joins, etc

## How do you use it?

By default, the KsqlTestFactory ```find*``` methods will look for *.ksql files.  
To specify a different file extension, specify the extension as the constructor argument:
```java
private final KsqlTestFactory ksqlTestFactory = new KsqlTestFactory("sql");
```

### Using findKsql* methods
```findKsqlTestCases``` and ```findKsqlNegativeTestCases``` methods will search the specified directory and all subdirectories.  
By default, ```findKsqlTestCases``` will look for input.json and output.json files in each directory where a ksql file is located and use them to confirm success or failure of a test.  
By default, ```findKsqlNegativeTestCases``` will look for negativeInput.json and negativeOutput.json files in each directory where a ksql file is located and use them to confirm success or failure of a test.  Use the ```findKsqlNegativeTestCases``` method when you want to run a negative test case to confirm that a test failed as expected.    

Input and output filenames can be specified as the second and third arguments of the methods.

The following locations are all starting locations / base directories checked for KSQL files and will be executed if present:
- project root
- src/main/ksql
- src/main/pipeline
- src/main/resources  

### Using runKsql* methods

```runKsqlTestCase``` and ```runKsqlNegativeTestCase``` methods will run only the specified test with the specified input/output files.  
Input and output file paths must be specified as the second and third arguments of the methods.

The following locations are all locations / base directories checked for KSQL files (in this order).  The input/output files are expected to have the same base directory as the test.  The test will be executed only in the first directory where it is found.
- project root
- src/main/ksql
- src/main/pipeline
- src/main/resources


```java
import org.hjug.ksqldb.testing.KsqlTestFactory;
        
class KsqlJavaTest {

    private final KsqlTestFactory ksqlTestFactory = new KsqlTestFactory();
    
    @TestFactory
    Stream<DynamicTest> testBarPipeline() {
        return ksqlTestFactory.findKsqlTestCases("ksql-samples/bar");
    }

    //specifying negativeInput.json and negativeOutput.json file names (this is optional)
    @TestFactory
    Stream<DynamicTest> testBazFailingPipeline() {
        return ksqlTestFactory.findKsqlNegativeTestCases("ksql-samples/baz", "negativeInput.json", "negativeOutput.json");
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
}
```

## Installation Instructions
This repository must currently be cloned and installed / deployed manually since it relies on the KSQL libraries.  The following repository must be added to your POM or Gradle file or your settings.xml file.
```xml
<repositories>
  <repository>
    <id>confluent</id>
    <name>confluent</name>
    <url>https://packages.confluent.io/maven/</url>
  </repository>
</repositories>
```

My sincere hope is that this will make its way into the ksqlDB codebase at some point in the future and the installation / deployment step will no longer be necessary.