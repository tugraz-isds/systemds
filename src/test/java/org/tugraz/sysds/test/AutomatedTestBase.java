/*
 * Modifications Copyright 2019 Graz University of Technology
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.tugraz.sysds.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.wink.json4j.JSONObject;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.tugraz.sysds.api.DMLScript;
import org.tugraz.sysds.common.Types.ExecMode;
import org.tugraz.sysds.conf.DMLConfig;
import org.tugraz.sysds.hops.OptimizerUtils;
import org.tugraz.sysds.lops.Lop;
import org.tugraz.sysds.lops.LopProperties.ExecType;
import org.tugraz.sysds.parser.DataExpression;
import org.tugraz.sysds.common.Types.DataType;
import org.tugraz.sysds.common.Types.ValueType;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.io.FileFormatPropertiesCSV;
import org.tugraz.sysds.runtime.io.FrameReader;
import org.tugraz.sysds.runtime.io.FrameReaderFactory;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.data.InputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.OutputInfo;
import org.tugraz.sysds.runtime.matrix.data.MatrixValue.CellIndex;
import org.tugraz.sysds.runtime.meta.MatrixCharacteristics;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.utils.ParameterBuilder;
import org.tugraz.sysds.utils.Statistics;


/**
 * <p>
 * Extend this class to easily
 * </p>
 * <ul>
 * <li>set up an environment for DML script execution</li>
 * <li>use multiple test cases</li>
 * <li>generate test data></li>
 * <li>check results</li>
 * <li>clean up after test run</li>
 * </ul>
 *
 */
@SuppressWarnings("deprecation")
public abstract class AutomatedTestBase
{
	public static final boolean EXCEPTION_EXPECTED = true;
	public static final boolean EXCEPTION_NOT_EXPECTED = false;

	// By default: TEST_GPU is set to false to allow developers without Nvidia GPU to run integration test suite
	public static final boolean TEST_GPU = false;
	public static final double GPU_TOLERANCE = 1e-9;

	// *** HACK ALERT *** HACK ALERT *** HACK ALERT ***
	// Hadoop 2.4.1 doesn't work on Windows unless winutils.exe is available
	// under $HADOOP_HOME/bin and hadoop.dll is available in the Java library
	// path. The following static initializer sets up JVM variables so that
	// Hadoop can find these native binaries, assuming that any Hadoop code
	// loads after this class and that the JVM's current working directory
	// is the root of this project.
	static {

		String osname = System.getProperty("os.name").toLowerCase();
		if (osname.contains("win")) {
			System.err.printf("AutomatedTestBase has detected a Windows OS and is overriding\n"
					+ "hadoop.home.dir and java.library.path.\n");
			String cwd = System.getProperty("user.dir");

			System.setProperty("hadoop.home.dir", cwd + File.separator
					+ "\\src\\test\\config\\hadoop_bin_windows");

			if(TEST_GPU) {
				String CUDA_LIBRARY_PATH = System.getenv("CUDA_PATH") + File.separator + "bin";
				System.setProperty("java.library.path", cwd + File.separator
						+ "\\src\\test\\config\\hadoop_bin_windows\\bin" + File.pathSeparator
						+ "/lib" + File.pathSeparator
						+ CUDA_LIBRARY_PATH);
			}
			else {
				System.setProperty("java.library.path", cwd + File.separator
					+ "\\src\\test\\config\\hadoop_bin_windows\\bin"
					// For testing BLAS on Windows
					// + File.pathSeparator + "C:\\Program Files (x86)\\IntelSWTools\\compilers_and_libraries_2017.0.109\\windows\\redist\\intel64_win\\mkl"
						);
			}


		    // Need to muck around with the classloader to get it to use the new
			// value of java.library.path.
			try {
				final Field sysPathsField = ClassLoader.class.getDeclaredField("sys_paths");
				sysPathsField.setAccessible(true);

				sysPathsField.set(null, null);
			} catch (Exception e) {
				// IBM Java throws an exception here, so don't print the stack trace.
				//e.printStackTrace();
				//System.err.printf("Caught exception while attempting to override library path. Attempting to continue.");
			}
		}
	}
	// *** END HACK ***

	/**
	 * Script source directory for .dml and .r files only
	 * (TEST_DATA_DIR for generated test data artifacts).
	 */
	protected static final String SCRIPT_DIR = "./src/test/scripts/";
	protected static final String INPUT_DIR = "in/";
	protected static final String OUTPUT_DIR = "out/";
	protected static final String EXPECTED_DIR = "expected/";

	/** Location where this class writes files for inspection if DEBUG is set to true. */
	private static final String DEBUG_TEMP_DIR = "./tmp/";

	/** Directory under which config files shared across tests are located. */
	private static final String CONFIG_DIR = "./src/test/config/";

	/**
	 * Location of the SystemDS config file that we use as a template when
	 * generating the configs for each test case.
	 */
	private static final File CONFIG_TEMPLATE_FILE = new File(CONFIG_DIR, "SystemDS-config.xml");

	/**
	 * Location under which we create local temporary directories for test cases.
	 * To adjust where testTemp is located, use -Dsystemds.testTemp.root.dir=<new location>.  This is necessary
	 * if any parent directories are public-protected.
	 */
	private static final String LOCAL_TEMP_ROOT_DIR = System.getProperty("systemds.testTemp.root.dir","target/testTemp");
	private static final File LOCAL_TEMP_ROOT = new File(LOCAL_TEMP_ROOT_DIR);

	/** Base directory for generated IN, OUT, EXPECTED test data artifacts instead of SCRIPT_DIR. */
	protected static final String TEST_DATA_DIR = LOCAL_TEMP_ROOT_DIR + "/";
	protected static final boolean TEST_CACHE_ENABLED = true;
	/** Optional sub-directory under EXPECTED_DIR for reusing R script test results */
	private String cacheDir = "";

	/**
	 * Runtime backend to use for all integration tests. Some individual tests
	 * override this value, but the rest will use whatever is the default here.
	 * <p>
	 * Also set DMLScript.USE_LOCAL_SPARK_CONFIG to true for running the test
	 * suite in spark mode
	 */
	protected static ExecMode rtplatform = ExecMode.HYBRID;

	protected static final boolean DEBUG = false;

	protected String fullDMLScriptName; // utilize for both DML and PyDML, should probably be renamed.
	// protected String fullPYDMLScriptName;
	protected String fullRScriptName;

	protected static String baseDirectory;
	protected static String sourceDirectory;
	protected HashMap<String, TestConfiguration> availableTestConfigurations;

	/* For testing in the old way */
	protected HashMap<String, String> testVariables; /* variables and their values */

	/* For testing in the new way */
	//protected String[] dmlArgs;            /* program-independent arguments to SystemDS (e.g., debug, execution mode) */
	protected String[] programArgs;        /* program-specific arguments, which are passed to SystemDS via -args option */
	protected String rCmd;                 /* Rscript foo.R arg1, arg2 ...          */

	protected String selectedTest;
	protected String[] outputDirectories;
	protected String[] comparisonFiles;
	protected ArrayList<String> inputDirectories;
	protected ArrayList<String> inputRFiles;
	protected ArrayList<String> expectedFiles;

	private File curLocalTempDir = null;



	private boolean isOutAndExpectedDeletionDisabled = false;
	private long lTimeBeforeTest = 0;

	private String expectedStdOut;
	private int iExpectedStdOutState = 0;
	private String unexpectedStdOut;
	private int iUnexpectedStdOutState = 0;
	private PrintStream originalPrintStreamStd = null;

	private String expectedStdErr;
	private int iExpectedStdErrState = 0;
	private PrintStream originalErrStreamStd = null;

	@Before
	public abstract void setUp();

	/**
	 * <p>
	 * Adds a test configuration to the list of available test configurations.
	 * </p>
	 *
	 * @param testName
	 *            test name
	 * @param config
	 *            test configuration
	 */
	protected void addTestConfiguration(String testName, TestConfiguration config) {
		availableTestConfigurations.put(testName, config);
	}
	
	protected void addTestConfiguration(TestConfiguration config) {
		availableTestConfigurations.put(config.getTestScript(), config);
	}

	/**
	 * <p>
	 * Adds a test configuration to the list of available test configurations based
	 * on the test directory and the test name.
	 * </p>
	 *
	 * @param testDirectory
	 *            test directory
	 * @param testName
	 *            test name
	 */
	protected void addTestConfiguration(String testDirectory, String testName) {
		TestConfiguration config = new TestConfiguration(testDirectory, testName);
		availableTestConfigurations.put(testName, config);
	}


	@Before
	public final void setUpBase() {
		availableTestConfigurations = new HashMap<String, TestConfiguration>();
		testVariables = new HashMap<String, String>();
		inputDirectories = new ArrayList<String>();
		inputRFiles = new ArrayList<String>();
		expectedFiles = new ArrayList<String>();
		outputDirectories = new String[0];
		setOutAndExpectedDeletionDisabled(false);
		lTimeBeforeTest = System.currentTimeMillis();

		TestUtils.clearAssertionInformation();
	}

	/**
	 * <p>
	 * Returns a test configuration from the list of available configurations.
	 * If no configuration is added for the specified name, the test will fail.
	 * </p>
	 *
	 * @param testName
	 *            test name
	 * @return test configuration
	 */
	protected TestConfiguration getTestConfiguration(String testName) {
		if (!availableTestConfigurations.containsKey(testName))
			fail("unable to load test configuration");

		return availableTestConfigurations.get(testName);
	}

	/**
	 * <p>
	 * Gets a test configuration from the list of available configurations
	 * and loads it if it's available. It is then returned.
	 * If no configuration exists for the specified name, the test will fail.
	 *
	 * </p>
	 *
	 * @param testName
	 *            test name
	 * @return test configuration
	 */
	protected TestConfiguration getAndLoadTestConfiguration(String testName) {
		TestConfiguration testConfiguration = getTestConfiguration(testName);
		loadTestConfiguration(testConfiguration);
		return testConfiguration;
	}

	/**
	 * Subclasses must call {@link #loadTestConfiguration(TestConfiguration)}
	 * before calling this method.
	 *
	 * @return the directory where the current test case should write temp
	 *         files. This directory also contains the current test's customized
	 *         SystemDS config file.
	 */
	protected File getCurLocalTempDir() {
		if (null == curLocalTempDir) {
			throw new RuntimeException(
					"Called getCurLocalTempDir() before calling loadTestConfiguration()");
		}
		return curLocalTempDir;
	}

	/**
	 * Subclasses must call {@link #loadTestConfiguration(TestConfiguration)}
	 * before calling this method.
	 *
	 * @return the location of the current test case's SystemDS config file
	 */
	protected File getCurConfigFile() {
		return new File(getCurLocalTempDir(), "SystemDS-config.xml");
	}

	/**
	 * <p>
	 * Tests that use custom SystemDS configuration should override to ensure
	 * scratch space and local temporary directory locations are also updated.
	 * </p>
	 */
	protected File getConfigTemplateFile() {
		return CONFIG_TEMPLATE_FILE;
	}

	protected ExecMode setExecMode(ExecType instType) {
		ExecMode platformOld = rtplatform;
		switch( instType ) {
			case SPARK: rtplatform = ExecMode.SPARK; break;
			default: rtplatform = ExecMode.HYBRID; break;
		}
		if( rtplatform != ExecMode.SINGLE_NODE )
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
		return platformOld;
	}
	
	/**
	 * <p>
	 * Generates a random matrix with the specified characteristics and returns
	 * it as a two dimensional array.
	 * </p>
	 *
	 * @param rows
	 *            number of rows
	 * @param cols
	 *            number of columns
	 * @param min
	 *            minimum value
	 * @param max
	 *            maximum value
	 * @param sparsity
	 *            sparsity
	 * @param seed
	 *            seed
	 * @return two dimensional array containing random matrix
	 */
	protected double[][] getRandomMatrix(int rows, int cols, double min, double max, double sparsity, long seed) {
		return TestUtils.generateTestMatrix(rows, cols, min, max, sparsity, seed);
	}

	/**
	 * <p>
	 * Generates a random matrix with the specified characteristics which does
	 * not contain any zero values and returns it as a two dimensional array.
	 * </p>
	 *
	 * @param rows
	 *            number of rows
	 * @param cols
	 *            number of columns
	 * @param min
	 *            minimum value
	 * @param max
	 *            maximum value
	 * @param seed
	 *            seed
	 * @return two dimensional array containing random matrix
	 */
	protected double[][] getNonZeroRandomMatrix(int rows, int cols, double min, double max, long seed) {
		return TestUtils.generateNonZeroTestMatrix(rows, cols, min, max, seed);
	}

	/**
	 * <p>
	 * Generates a random matrix with the specified characteristics and writes
	 * it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param rows
	 *            number of rows
	 * @param cols
	 *            number of columns
	 * @param min
	 *            minimum value
	 * @param max
	 *            maximum value
	 * @param sparsity
	 *            sparsity
	 * @param seed
	 *            seed
	 */
	protected double[][] createRandomMatrix(String name, int rows, int cols, double min, double max, double sparsity,
			long seed) {
		return createRandomMatrix(name, rows, cols, min, max, sparsity, seed, false);
	}

	/**
	 * <p>
	 * Generates a random matrix with the specified characteristics and writes
	 * it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param rows
	 *            number of rows
	 * @param cols
	 *            number of columns
	 * @param min
	 *            minimum value
	 * @param max
	 *            maximum value
	 * @param sparsity
	 *            sparsity
	 * @param seed
	 *            seed
	 * @param bIncludeR
	 *            If true, writes also a R matrix to disk
	 */
	protected double[][] createRandomMatrix(String name, int rows, int cols, double min, double max, double sparsity,
			long seed, boolean bIncludeR) {
		double[][] matrix = TestUtils.generateTestMatrix(rows, cols, min, max, sparsity, seed);
		String completePath = baseDirectory + INPUT_DIR + name + "/in";

		TestUtils.writeTestMatrix(completePath, matrix, bIncludeR);
		if (DEBUG)
			TestUtils.writeTestMatrix(DEBUG_TEMP_DIR + completePath, matrix);
		inputDirectories.add(baseDirectory + INPUT_DIR + name);
		return matrix;
	}

	private static void cleanupExistingData(String fname, boolean cleanupRData) throws IOException {
		HDFSTool.deleteFileIfExistOnHDFS(fname);
		HDFSTool.deleteFileIfExistOnHDFS(fname + ".mtd");
		if ( cleanupRData )
			HDFSTool.deleteFileIfExistOnHDFS(fname + ".mtx");
	}

	/**
	 * <p>
	 * Adds a matrix to the input path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 * @param bIncludeR
	 *            generates also the corresponding R matrix
	 */
	protected double[][] writeInputMatrix(String name, double[][] matrix, boolean bIncludeR) {
		String completePath = baseDirectory + INPUT_DIR + name + "/in";
		String completeRPath = baseDirectory + INPUT_DIR + name + ".mtx";

		try {
			cleanupExistingData(baseDirectory + INPUT_DIR + name, bIncludeR);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		TestUtils.writeTestMatrix(completePath, matrix);
		if (bIncludeR) {
			TestUtils.writeTestMatrix(completeRPath, matrix, true);
			inputRFiles.add(completeRPath);
		}
		if (DEBUG)
			TestUtils.writeTestMatrix(DEBUG_TEMP_DIR + completePath, matrix);
		inputDirectories.add(baseDirectory + INPUT_DIR + name);

		return matrix;
	}

	protected double[][] writeInputMatrixWithMTD(String name, MatrixBlock matrix, boolean bIncludeR) {
		double[][] data = DataConverter.convertToDoubleMatrix(matrix);
		return writeInputMatrixWithMTD(name, data, bIncludeR);
	}
	
	protected double[][] writeInputMatrixWithMTD(String name, double[][] matrix, boolean bIncludeR) {
		MatrixCharacteristics mc = new MatrixCharacteristics(matrix.length, matrix[0].length, OptimizerUtils.DEFAULT_BLOCKSIZE, OptimizerUtils.DEFAULT_BLOCKSIZE, -1);
		return writeInputMatrixWithMTD(name, matrix, bIncludeR, mc);
	}

	protected double[][] writeInputMatrixWithMTD(String name, double[][] matrix, long nnz, boolean bIncludeR) {
		MatrixCharacteristics mc = new MatrixCharacteristics(matrix.length, matrix[0].length, OptimizerUtils.DEFAULT_BLOCKSIZE, OptimizerUtils.DEFAULT_BLOCKSIZE, nnz);
		return writeInputMatrixWithMTD(name, matrix, bIncludeR, mc);
	}

	protected double[][] writeInputMatrixWithMTD(String name, double[][] matrix, boolean bIncludeR, MatrixCharacteristics mc)
	{
		writeInputMatrix(name, matrix, bIncludeR);

		// write metadata file
		try
		{
			String completeMTDPath = baseDirectory + INPUT_DIR + name + ".mtd";
			HDFSTool.writeMetaDataFile(completeMTDPath, ValueType.FP64, mc, OutputInfo.stringToOutputInfo("textcell"));
		}
		catch(IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		return matrix;
	}

	/**
	 * <p>
	 * Adds a matrix to the input path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 */
	protected double[][] writeInputMatrix(String name, double[][] matrix) {
		return writeInputMatrix(name, matrix, false);
	}

	/**
	 * <p>
	 * Adds a matrix to the input path and writes it to a file in binary format.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 * @param rowsInBlock
	 *            rows in block
	 * @param colsInBlock
	 *            columns in block
	 * @param sparseFormat
	 *            sparse format
	 */
	protected void writeInputBinaryMatrix(String name, double[][] matrix, int rowsInBlock, int colsInBlock,
			boolean sparseFormat) {
		String completePath = baseDirectory + INPUT_DIR + name + "/in";

		try {
			cleanupExistingData(baseDirectory + INPUT_DIR + name, false);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		if (rowsInBlock == 1 && colsInBlock == 1) {
			TestUtils.writeBinaryTestMatrixCells(completePath, matrix);
			if (DEBUG)
				TestUtils.writeBinaryTestMatrixCells(DEBUG_TEMP_DIR + completePath, matrix);
		} else {
			TestUtils.writeBinaryTestMatrixBlocks(completePath, matrix, rowsInBlock, colsInBlock, sparseFormat);
			if (DEBUG)
				TestUtils.writeBinaryTestMatrixBlocks(DEBUG_TEMP_DIR + completePath, matrix, rowsInBlock, colsInBlock,
						sparseFormat);
		}
		inputDirectories.add(baseDirectory + INPUT_DIR + name);
	}

	/**
	 * Writes the given matrix to input path, and writes the associated metadata file.
	 *
	 * @param name
	 * @param matrix
	 * @param rowsInBlock
	 * @param colsInBlock
	 * @param sparseFormat
	 * @param mc
	 * @throws IOException
	 */
	protected void writeInputBinaryMatrixWithMTD(String name, double[][] matrix, int rowsInBlock, int colsInBlock,
			boolean sparseFormat, MatrixCharacteristics mc) throws IOException {
		writeInputBinaryMatrix(name, matrix, rowsInBlock, colsInBlock, sparseFormat);
		// write metadata file
		String completeMTDPath = baseDirectory + INPUT_DIR + name + ".mtd";
		HDFSTool.writeMetaDataFile(completeMTDPath, ValueType.FP64, mc, OutputInfo.stringToOutputInfo("binaryblock"));
	}

	/**
	 * <p>
	 * Adds a matrix to the expectation path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 */
	protected void writeExpectedMatrix(String name, double[][] matrix) {
		TestUtils.writeTestMatrix(baseDirectory + EXPECTED_DIR + cacheDir + name, matrix);
		expectedFiles.add(baseDirectory + EXPECTED_DIR + cacheDir + name);
	}

	/**
	 * <p>
	 * Adds a matrix to the expectation path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 */
	protected void writeExpectedMatrixMarket(String name, double[][] matrix) {
		File path = new File(baseDirectory, EXPECTED_DIR + cacheDir);
		path.mkdirs();

		TestUtils.writeTestMatrix(baseDirectory + EXPECTED_DIR + cacheDir + name, matrix, true);
		expectedFiles.add(baseDirectory + EXPECTED_DIR + cacheDir + name);
	}
	/**
	 * <p>
	 * Adds a matrix to the expectation path and writes it to a file in binary
	 * format.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param matrix
	 *            two dimensional matrix
	 * @param rowsInBlock
	 *            rows in block
	 * @param colsInBlock
	 *            columns in block
	 * @param sparseFormat
	 *            sparse format
	 */
	protected void writeExpectedBinaryMatrix(String name, double[][] matrix, int rowsInBlock, int colsInBlock,
			boolean sparseFormat) {
		if (rowsInBlock == 1 && colsInBlock == 1) {
			TestUtils.writeBinaryTestMatrixCells(baseDirectory + EXPECTED_DIR + name + "/in", matrix);
		} else {
			TestUtils.writeBinaryTestMatrixBlocks(baseDirectory + EXPECTED_DIR + name + "/in", matrix, rowsInBlock,
					colsInBlock, sparseFormat);
		}
		inputDirectories.add(baseDirectory + EXPECTED_DIR + name);
	}

	/**
	 * <p>
	 * Creates a helper matrix which can be used for writing scalars to a file.
	 * </p>
	 */
	protected void createHelperMatrix() {
		TestUtils.writeTestMatrix(baseDirectory + INPUT_DIR + "helper/in", new double[][] { { 1, 1 } });
		inputDirectories.add(baseDirectory + INPUT_DIR + "helper");
	}

	/**
	 * <p>
	 * Creates a expectation helper matrix which can be used to compare scalars.
	 * </p>
	 *
	 * @param name
	 *            file name
	 * @param value
	 *            scalar value
	 */
	protected void writeExpectedHelperMatrix(String name, double value) {
		TestUtils.writeTestMatrix(baseDirectory + EXPECTED_DIR + cacheDir + name, new double[][] { { value, value } });
		expectedFiles.add(baseDirectory + EXPECTED_DIR + cacheDir + name);
	}

	protected void writeExpectedScalar(String name, double value) {
		File path = new File(baseDirectory, EXPECTED_DIR + cacheDir);
		path.mkdirs();

		TestUtils.writeTestScalar(baseDirectory + EXPECTED_DIR + cacheDir + name, value);
		expectedFiles.add(baseDirectory + EXPECTED_DIR + cacheDir + name);
	}

	protected void writeExpectedScalar(String name, long value) {
		File path = new File(baseDirectory, EXPECTED_DIR + cacheDir);
		path.mkdirs();

		TestUtils.writeTestScalar(baseDirectory + EXPECTED_DIR + cacheDir + name, value);
		expectedFiles.add(baseDirectory + EXPECTED_DIR + cacheDir + name);
	}

	protected static HashMap<CellIndex, Double> readDMLMatrixFromHDFS(String fileName) {
		return TestUtils.readDMLMatrixFromHDFS(baseDirectory + OUTPUT_DIR + fileName);
	}

	public HashMap<CellIndex, Double> readRMatrixFromFS(String fileName) {
		System.out.println("R script out: " + baseDirectory + EXPECTED_DIR + cacheDir + fileName);
		return TestUtils.readRMatrixFromFS(baseDirectory + EXPECTED_DIR + cacheDir + fileName);
	}

	protected static HashMap<CellIndex, Double> readDMLScalarFromHDFS(String fileName) {
		return TestUtils.readDMLScalarFromHDFS(baseDirectory + OUTPUT_DIR + fileName);
	}

	protected static String readDMLLineageFromHDFS(String fileName) {
		return TestUtils.readDMLString(baseDirectory + OUTPUT_DIR + fileName + ".lineage");
	}


	protected static FrameBlock readDMLFrameFromHDFS(String fileName, InputInfo iinfo) throws IOException {
		//read frame data from hdfs
		String strFrameFileName = baseDirectory + OUTPUT_DIR + fileName;
		FrameReader reader = FrameReaderFactory.createFrameReader(iinfo);

		MatrixCharacteristics md = readDMLMetaDataFile(fileName);
		return reader.readFrameFromHDFS(strFrameFileName, md.getRows(), md.getCols());
	}


	protected static FrameBlock readDMLFrameFromHDFS(String fileName, InputInfo iinfo, MatrixCharacteristics md) throws IOException {
		//read frame data from hdfs
		String strFrameFileName = baseDirectory + OUTPUT_DIR + fileName;
		FrameReader reader = FrameReaderFactory.createFrameReader(iinfo);

		return reader.readFrameFromHDFS(strFrameFileName, md.getRows(), md.getCols());
	}

	protected static FrameBlock readRFrameFromHDFS(String fileName, InputInfo iinfo, MatrixCharacteristics md) throws IOException {
		//read frame data from hdfs
		String strFrameFileName = baseDirectory + EXPECTED_DIR + fileName;

		FileFormatPropertiesCSV fprop = new FileFormatPropertiesCSV();
		fprop.setHeader(true);
		FrameReader reader = FrameReaderFactory.createFrameReader(iinfo, fprop);

		return reader.readFrameFromHDFS(strFrameFileName, md.getRows(), md.getCols());
	}



	public HashMap<CellIndex, Double> readRScalarFromFS(String fileName) {
		System.out.println("R script out: " + baseDirectory + EXPECTED_DIR + cacheDir + fileName);
		return TestUtils.readRScalarFromFS(baseDirectory + EXPECTED_DIR + cacheDir + fileName);
	}

	public static void checkDMLMetaDataFile(String fileName, MatrixCharacteristics mc) {
		MatrixCharacteristics rmc = readDMLMetaDataFile(fileName);
		Assert.assertEquals(mc.getRows(), rmc.getRows());
		Assert.assertEquals(mc.getCols(), rmc.getCols());
	}

	public static MatrixCharacteristics readDMLMetaDataFile(String fileName)
	{
		try {
			String fname = baseDirectory + OUTPUT_DIR + fileName +".mtd";
			JSONObject meta = new DataExpression().readMetadataFile(fname, false);
			long rlen = Long.parseLong(meta.get(DataExpression.READROWPARAM).toString());
			long clen = Long.parseLong(meta.get(DataExpression.READCOLPARAM).toString());
			return new MatrixCharacteristics(rlen, clen, -1, -1);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	public static ValueType readDMLMetaDataValueType(String fileName)
	{
		try {
			String fname = baseDirectory + OUTPUT_DIR + fileName +".mtd";
			JSONObject meta = new DataExpression().readMetadataFile(fname, false);
			return ValueType.fromExternalString(meta.get(DataExpression.VALUETYPEPARAM).toString());
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	/**
	 * <p>
	 * Loads a test configuration with its parameters. Adds the output
	 * directories to the output list as well as to the list of possible
	 * comparison files.
	 * </p>
	 *
	 * @param config
	 *            test configuration name
	 *
	 */
	protected void loadTestConfiguration(TestConfiguration config) {
		loadTestConfiguration(config, null);
	}

	/**
	 * <p>
	 * Loads a test configuration with its parameters. Adds the output
	 * directories to the output list as well as to the list of possible
	 * comparison files.
	 * </p>
	 *
	 * @param config
	 *            test configuration name
	 * @param cacheDirectory
	 *            subdirectory for reusing R script expected results
	 *            if null, defaults to empty string (i.e., no cache)
	 */
	protected void loadTestConfiguration(TestConfiguration config, String cacheDirectory) {
		if (!availableTestConfigurations.containsValue(config))
			fail("test configuration not available: " + config.getTestScript());
		String testDirectory = config.getTestDirectory();
		if (testDirectory != null) {
			if (isTargetTestDirectory(testDirectory)) {
				baseDirectory = TEST_DATA_DIR + testDirectory;
				sourceDirectory = SCRIPT_DIR + getSourceDirectory(testDirectory);
			}
			else {
				baseDirectory = SCRIPT_DIR + testDirectory;
				sourceDirectory = baseDirectory;
			}
		}

		setCacheDirectory(cacheDirectory);

		selectedTest = config.getTestScript();

		String[] outputFiles = config.getOutputFiles();
		if (outputFiles != null) {
			outputDirectories = new String[outputFiles.length];
			comparisonFiles = new String[outputFiles.length];
			for (int i = 0; i < outputFiles.length; i++) {
				outputDirectories[i] = baseDirectory + OUTPUT_DIR + outputFiles[i];
				comparisonFiles[i] = baseDirectory + EXPECTED_DIR + cacheDir + outputFiles[i];
			}
		}

		testVariables = config.getVariables();
		testVariables.put("basedir", baseDirectory);
		testVariables.put("indir", baseDirectory + INPUT_DIR);
		testVariables.put("outdir", baseDirectory + OUTPUT_DIR);
		testVariables.put("readhelper", "Helper = read(\"" + baseDirectory + INPUT_DIR + "helper/in\", "
				+ "rows=1, cols=2, format=\"text\");");
		testVariables.put("Routdir", baseDirectory + EXPECTED_DIR + cacheDir);

		// Create a temporary directory for this test case.
		// Eventually all files written by the tests should go under here, but making
		// that change will take quite a bit of effort.
		try {
			if (null == testDirectory) {
				System.err
						.printf("Warning: Test configuration did not specify a test directory.\n");
				curLocalTempDir = new File(LOCAL_TEMP_ROOT, String.format(
						"unknownTest/%s", selectedTest));
			} else {
				curLocalTempDir = new File(LOCAL_TEMP_ROOT, String.format(
						"%s/%s", testDirectory, selectedTest));
			}

			curLocalTempDir.mkdirs();
			TestUtils.clearDirectory(curLocalTempDir.getPath());

			// Create a SystemDS config file for this test case based on default template
			// from src/test/config or derive from custom configuration provided by test.
			String configTemplate = FileUtils.readFileToString(getConfigTemplateFile(), "UTF-8");
			String localTemp = curLocalTempDir.getPath();
			String configContents = configTemplate
				.replace(createXMLElement(DMLConfig.SCRATCH_SPACE, "scratch_space"),
					createXMLElement(DMLConfig.SCRATCH_SPACE, localTemp+"/scratch_space"))
				.replace(createXMLElement(DMLConfig.LOCAL_TMP_DIR, "/tmp/systemds"),
					createXMLElement(DMLConfig.LOCAL_TMP_DIR, localTemp+"/localtmp"));
			
			FileUtils.write(getCurConfigFile(), configContents, "UTF-8");

			System.out.printf("This test case will use SystemDS config file %s\n", getCurConfigFile());
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		if (DEBUG)
			TestUtils.clearDirectory(DEBUG_TEMP_DIR + baseDirectory + INPUT_DIR);
	}

	public String createXMLElement(String tagName, String value) {
		return String.format("<%s>%s</%s>",tagName, value, tagName);
	}

	/**
	 * <p>
	 * Loads a test configuration with its parameters. Adds the output
	 * directories to the output list as well as to the list of possible
	 * comparison files.
	 * </p>
	 *
	 * @param configurationName
	 *            test configuration name
	 *
	 */
	@Deprecated
	protected void loadTestConfiguration(String configurationName) {
		if (!availableTestConfigurations.containsKey(configurationName))
			fail("test configuration not available: " + configurationName);

		TestConfiguration config = availableTestConfigurations.get(configurationName);

		loadTestConfiguration(config);
	}

	/**
	 * Runs an R script, default to the old way
	 */
	protected void runRScript() {
		runRScript(false);

	}
	/**
	 * Runs an R script in the old or the new way
	 */
	protected void runRScript(boolean newWay) {

		String executionFile = sourceDirectory + selectedTest + ".R";

		// *** HACK ALERT *** HACK ALERT *** HACK ALERT ***
		// Some of the R scripts will fail if the "expected" directory doesn't exist.
		// Make sure the directory exists.
		File expectedDir = new File(baseDirectory, "expected" + "/" + cacheDir);
		expectedDir.mkdirs();
		// *** END HACK ***

		String cmd;
		if( !newWay ) {
			executionFile = executionFile + "t";
			cmd = "R -f " + executionFile;
		}
		else {
			// *** HACK ALERT *** HACK ALERT *** HACK ALERT ***
			// Rscript does *not* load the "methods" package by default
			// to save on start time. The "Matrix" package used in the
			// tests requires the "methods" package and should still
			// load and attach it, but in R 3.2 with the latest version
			// of the "Matrix" package, "methods" is loaded *but not
			// attached* when run with Rscript.  Therefore, we need to
			// explicitly load it with Rscript.
			cmd = rCmd.replaceFirst("Rscript",
					"Rscript --default-packages=methods,datasets,graphics,grDevices,stats,utils");
			// *** END HACK ***
		}

		if (System.getProperty("os.name").contains("Windows")) {
			cmd = cmd.replace('/', '\\');
			executionFile = executionFile.replace('/', '\\');
		}
		if (DEBUG) {
			if( !newWay ) { // not sure why have this condition
				TestUtils.printRScript(executionFile);
			}
		}
		if( !newWay ) {
			ParameterBuilder.setVariablesInScript(sourceDirectory, selectedTest + ".R", testVariables);
		}

		if (cacheDir.length() > 0)
		{
			File expectedFile = null;
			String[] outputFiles = null;
			TestConfiguration testConfig = getTestConfiguration(selectedTest);
			if (testConfig != null)
			{
				outputFiles = testConfig.getOutputFiles();
			}

			if (outputFiles != null && outputFiles.length > 0)
			{
				expectedFile = new File (expectedDir.getPath() + "/" + outputFiles[0]);
				if (expectedFile.canRead())
				{
					System.out.println("Skipping R script cmd: " + cmd);
					return;
				}
			}
		}

		try {
			long t0 = System.nanoTime();
			System.out.println("starting R script");
			System.out.println("cmd: " + cmd);
			Process child = Runtime.getRuntime().exec(cmd);

			String outputR = IOUtils.toString(child.getInputStream());
			System.out.println("Standard Output from R:" + outputR);
			String errorString = IOUtils.toString(child.getErrorStream());
			System.err.println("Standard Error from R:" + errorString);

			//
			// To give any stream enough time to print all data, otherwise there
			// are situations where the test case fails, even before everything
			// has been printed
			//
			child.waitFor();
	//		Thread.sleep(30000);

			try {
				if (child.exitValue() != 0) {
					throw new Exception("ERROR: R has ended irregularly\n" + outputR + "\nscript file: "
							+ executionFile);
				}
			} catch (IllegalThreadStateException ie) {
				//
				// In UNIX JVM does not seem to be able to close threads
				// correctly. However, give it a try, since R processed the
				// script, therefore we can terminate the process.
				//
				child.destroy();
			}

			long t1 = System.nanoTime();
			System.out.println("R is finished (in "+((double)t1-t0)/1000000000+" sec)");

		} catch (Exception e) {
			e.printStackTrace();
			StringBuilder errorMessage = new StringBuilder();
			errorMessage.append("failed to run script " + executionFile);
			errorMessage.append("\nexception: " + e.toString());
			errorMessage.append("\nmessage: " + e.getMessage());
			errorMessage.append("\nstack trace:");
			for (StackTraceElement ste : e.getStackTrace()) {
				errorMessage.append("\n>" + ste);
			}
			fail(errorMessage.toString());
		}
	}

	/**
	 * <p>
	 * Runs a test for which no exception is expected.
	 * </p>
	 */
	protected void runTest() {
		runTest(false, null);
	}

	/**
	 * <p>
	 * Runs a test for which no exception is expected. If SystemDS executes more
	 * MR jobs than specified in maxMRJobs this test will fail.
	 * </p>
	 *
	 * @param maxMRJobs
	 *            specifies a maximum limit for the number of MR jobs. If set to
	 *            -1 there is no limit.
	 */
	protected void runTest(int maxMRJobs) {
		runTest(false, null, maxMRJobs);
	}

	/**
	 * <p>
	 * Runs a test for which the exception expectation can be specified.
	 * </p>
	 *
	 * @param exceptionExpected
	 *            exception expected
	 */
	protected void runTest(boolean exceptionExpected) {
		runTest(exceptionExpected, null);
	}

	/**
	 * <p>
	 * Runs a test for which the exception expectation can be specified as well
	 * as the specific expectation which is expected.
	 * </p>
	 *
	 * @param exceptionExpected
	 *            exception expected
	 * @param expectedException
	 *            expected exception
	 */
	protected void runTest(boolean exceptionExpected, Class<?> expectedException) {
		runTest(exceptionExpected, expectedException, -1);
	}

	/**
	 * <p>
	 * Runs a test for which the exception expectation can be specified as well
	 * as the specific expectation which is expected. If SystemDS executes more
	 * MR jobs than specified in maxMRJobs this test will fail.
	 * </p>
	 *
	 * @param exceptionExpected
	 *            exception expected
	 * @param expectedException
	 *            expected exception
	 * @param maxMRJobs
	 *            specifies a maximum limit for the number of MR jobs. If set to
	 *            -1 there is no limit.
	 */
	protected void runTest(boolean exceptionExpected, Class<?> expectedException, int maxMRJobs) {
		runTest(false, exceptionExpected, expectedException, null, maxMRJobs);
	}

	/**
	 * <p>
	 * Runs a test for which the exception expectation can be specified as well
	 * as the specific expectation which is expected. If SystemDS executes more
	 * MR jobs than specified in maxMRJobs this test will fail.
	 * </p>
	 * @param newWay
	 * 			  in the new way if it is set to true
	 * @param exceptionExpected
	 *            exception expected
	 * @param expectedException
	 *            expected exception
	 * @param maxMRJobs
	 *            specifies a maximum limit for the number of MR jobs. If set to
	 *            -1 there is no limit.
	 */
	protected void runTest(boolean newWay, boolean exceptionExpected, Class<?> expectedException, int maxMRJobs) {
		runTest(newWay, exceptionExpected, expectedException, null, maxMRJobs);
	}

	/**
	 * <p>
	 * Runs a test for which the exception expectation and the error message
	 * can be specified as well as the specific expectation which is expected.
	 * If SystemDS executes more MR jobs than specified in maxMRJobs this test
	 * will fail.
	 * </p>
	 * @param newWay
	 * 			  in the new way if it is set to true
	 * @param exceptionExpected
	 *            exception expected
	 * @param expectedException
	 *            expected exception
	 * @param errMessage
	 * 		      expected error message
	 * @param maxSparkInst
	 *            specifies a maximum limit for the number of MR jobs. If set to
	 *            -1 there is no limit.
	 */
	protected void runTest(boolean newWay, boolean exceptionExpected, Class<?> expectedException, String errMessage, int maxSparkInst) {

		String executionFile = sourceDirectory + selectedTest + ".dml";

		if( !newWay ) {
			executionFile = executionFile + "t";
			ParameterBuilder.setVariablesInScript(sourceDirectory, selectedTest + ".dml", testVariables);
		}

		//cleanup scratch folder (prevent side effect between tests)
		cleanupScratchSpace();

		ArrayList<String> args = new ArrayList<String>();
		// setup arguments to SystemDS

		if (DEBUG) {
			args.add("-Dsystemds.logging=trace");
		}

		if (newWay) {
			// Need a null pointer check because some tests read DML from a string.
			if (null != fullDMLScriptName) {
				args.add("-f");
				args.add(fullDMLScriptName);
			}
		} else {
			if (null != executionFile) {
				args.add("-f");
				args.add(executionFile);
			}
		}
		// program-independent parameters
		args.add("-exec");
		if (rtplatform == ExecMode.HYBRID) {
			DMLScript.USE_LOCAL_SPARK_CONFIG = true;
			args.add("hybrid");
		}
		else if (rtplatform == ExecMode.SINGLE_NODE)
			args.add("singlenode");
		else if (rtplatform == ExecMode.SPARK)
			args.add("spark");
		else {
			throw new RuntimeException("Unknown runtime platform: " + rtplatform);
		}
		//use optional config file since default under SystemDS/DML
		args.add("-config");
		args.add(getCurConfigFile().getPath());

		if(TEST_GPU)
			args.add("-gpu");

		// program-specific parameters
		if ( newWay ) {
			for (int i=0; i < programArgs.length; i++)
				args.add(programArgs[i]);
		}


		if (DEBUG) {
			if ( !newWay )
				TestUtils.printDMLScript(executionFile);
			else {
				TestUtils.printDMLScript(fullDMLScriptName);
			}
		}

		try {
			String [] dmlScriptArgs = args.toArray(new String[args.size()]);
			System.out.println("arguments to DMLScript: " + Arrays.toString(dmlScriptArgs));
			DMLScript.main(dmlScriptArgs);

			if (maxSparkInst > -1 && maxSparkInst < Statistics.getNoOfCompiledSPInst())
				fail("Limit of Spark jobs is exceeded: expected: " + maxSparkInst + ", occurred: "
						+ Statistics.getNoOfCompiledSPInst());

			if (exceptionExpected)
				fail("expected exception which has not been raised: " + expectedException);
		} catch (Exception e) {
			if (errMessage != null && !errMessage.equals("")) {
				boolean result = rCompareException(exceptionExpected, errMessage, e, false);
				if (exceptionExpected && !result) {
					fail(String.format("expected exception message '%s' has not been raised.", errMessage));
				}
			}
			if (!exceptionExpected || (expectedException != null && !(e.getClass().equals(expectedException)))) {
				e.printStackTrace();
				StringBuilder errorMessage = new StringBuilder();
				errorMessage.append("failed to run script " + executionFile);
				errorMessage.append("\nexception: " + e.toString());
				errorMessage.append("\nmessage: " + e.getMessage());
				errorMessage.append("\nstack trace:");
				for (StackTraceElement ste : e.getStackTrace()) {
					errorMessage.append("\n>" + ste);
				}
				fail(errorMessage.toString());
			}
		}
	}

	private boolean rCompareException(boolean exceptionExpected, String errMessage, Throwable e, boolean result) {
		if (e.getCause() != null) {
			result |= rCompareException(exceptionExpected, errMessage, e.getCause(), result);
		}
		if (exceptionExpected && errMessage != null && e.getMessage().contains(errMessage)) {
			result = true;
		}
		return result;
	}

	public void cleanupScratchSpace()
	{
		try
		{
			//parse config file
			DMLConfig conf = new DMLConfig(getCurConfigFile().getPath());

			// delete the scratch_space and all contents
			// (prevent side effect between tests)
			String dir = conf.getTextValue(DMLConfig.SCRATCH_SPACE);
			HDFSTool.deleteFileIfExistOnHDFS(dir);
		}
		catch (Exception ex)
		{
			//ex.printStackTrace();
			return; //no effect on tests
		}
	}

	/**
	 * <p>
	 * Checks if a process-local temporary directory exists
	 * in the current working directory.
	 * </p>
	 *
	 * @return true if a process-local temp directory is present.
	 */
	public boolean checkForProcessLocalTemporaryDir() {
		try {
			DMLConfig conf = new DMLConfig(getCurConfigFile().getPath());

			StringBuilder sb = new StringBuilder();
			sb.append(conf.getTextValue(DMLConfig.SCRATCH_SPACE));
			sb.append(Lop.FILE_SEPARATOR);
			sb.append(Lop.PROCESS_PREFIX);
			sb.append(DMLScript.getUUID());
			String pLocalDir = sb.toString();

			return HDFSTool.existsFileOnHDFS(pLocalDir);
		} catch (Exception ex) {
			ex.printStackTrace();
			return true;
		}
	}

	/**
	 * <p>
	 * Compares the results of the computation with the expected ones.
	 * </p>
	 */
	protected void compareResults() {
		compareResults(0);
	}

	/**
	 * <p>
	 * Compares the results of the computation with the expected ones with a
	 * specified tolerance.
	 * </p>
	 *
	 * @param epsilon
	 *            tolerance
	 */
	protected void compareResultsWithR(double epsilon) {
		for (int i = 0; i < comparisonFiles.length; i++) {
			TestUtils.compareDMLHDFSFileWithRFile(comparisonFiles[i], outputDirectories[i], epsilon);
		}
	}

	/**
	 * <p>
	 * Compares the results of the computation with the Result calculated by R
	 * </p>
	 */
	protected void compareResultsWithR() {
		compareResultsWithR(0);
	}

	protected void compareResultsWithMM () {
		TestUtils.compareMMMatrixWithJavaMatrix (comparisonFiles[0], outputDirectories[0], 0);
	}
	/**
	 * <p>
	 * Compares the results of the computation with the expected ones with a
	 * specified tolerance.
	 * </p>
	 *
	 * @param epsilon
	 *            tolerance
	 */
	protected void compareResults(double epsilon) {
		for (int i = 0; i < comparisonFiles.length; i++) {
			/* Note that DML scripts may generate a file with only scalar value */
			if (outputDirectories[i].endsWith(".scalar")) {
			   String javaFile = comparisonFiles[i].replace(".scalar", "");
			   String dmlFile = outputDirectories[i].replace(".scalar", "");
			   TestUtils.compareDMLScalarWithJavaScalar(javaFile, dmlFile, epsilon);
			}
			else {
				TestUtils.compareDMLMatrixWithJavaMatrix(comparisonFiles[i], outputDirectories[i], epsilon);
			}
		}
	}


	/**
	 * Compare results of the computation with the expected results where rows may be permuted.
	 * @param epsilon
	 */
	protected void compareResultsRowsOutOfOrder(double epsilon)
	{
		for (int i = 0; i < comparisonFiles.length; i++) {
			/* Note that DML scripts may generate a file with only scalar value */
			if (outputDirectories[i].endsWith(".scalar")) {
			   String javaFile = comparisonFiles[i].replace(".scalar", "");
			   String dmlFile = outputDirectories[i].replace(".scalar", "");
			   TestUtils.compareDMLScalarWithJavaScalar(javaFile, dmlFile, epsilon);
			}
			else {
				TestUtils.compareDMLMatrixWithJavaMatrixRowsOutOfOrder(comparisonFiles[i], outputDirectories[i], epsilon);
			}
		}
	}

	/**
	 * Checks that the number of Spark instructions that the current test case has
	 * compiled is equal to the expected number. Generates a JUnit error message
	 * if the number is out of line.
	 *
	 * @param expectedNumCompiled
	 *            number of Spark instructions that the current test case is
	 *            expected to compile
	 */
	protected void checkNumCompiledSparkInst(int expectedNumCompiled) {
		assertEquals("Unexpected number of compiled Spark instructions.",
				expectedNumCompiled, Statistics.getNoOfCompiledSPInst());
	}

	/**
	 * Checks that the number of Spark instructions that the current test case has
	 * executed (as opposed to compiling into the execution plan) is equal to
	 * the expected number. Generates a JUnit error message if the number is out
	 * of line.
	 *
	 * @param expectedNumExecuted
	 *            number of Spark instructions that the current test case is
	 *            expected to run
	 */
	protected void checkNumExecutedSparkInst(int expectedNumExecuted) {
		assertEquals("Unexpected number of executed Spark instructions.",
				expectedNumExecuted, Statistics.getNoOfExecutedSPInst());
	}

	/**
	 * <p>
	 * Checks the results of a computation against a number of characteristics.
	 * </p>
	 *
	 * @param rows
	 *            number of rows
	 * @param cols
	 *            number of columns
	 * @param min
	 *            minimum value
	 * @param max
	 *            maximum value
	 */
	protected void checkResults(long rows, long cols, double min, double max) {
		for (int i = 0; i < outputDirectories.length; i++) {
			TestUtils.checkMatrix(outputDirectories[i], rows, cols, min, max);
		}
	}

	/**
	 * <p>
	 * Checks for the existence for all of the outputs.
	 * </p>
	 */
	protected void checkForResultExistence() {
		for (int i = 0; i < outputDirectories.length; i++) {
			TestUtils.checkForOutputExistence(outputDirectories[i]);
		}
	}

	@After
	public void tearDown() {
		System.out.println("Duration: " + (System.currentTimeMillis() - lTimeBeforeTest) + "ms");


		assertTrue("expected String did not occur: " + expectedStdOut, iExpectedStdOutState == 0
				|| iExpectedStdOutState == 2);
		assertTrue("expected String did not occur (stderr): " + expectedStdErr, iExpectedStdErrState == 0
				|| iExpectedStdErrState == 2);
		assertFalse("unexpected String occurred: " + unexpectedStdOut, iUnexpectedStdOutState == 1);
		TestUtils.displayAssertionBuffer();


		if (!isOutAndExpectedDeletionDisabled()) {
			TestUtils.removeHDFSDirectories(inputDirectories.toArray(new String[inputDirectories.size()]));
			TestUtils.removeFiles(inputRFiles.toArray(new String[inputRFiles.size()]));

			// The following cleanup code is disabled (see [SYSTEMML-256]) until we can figure out
			// what test cases are creating temporary directories at the root of the project.
			//TestUtils.removeTemporaryFiles();

			TestUtils.clearDirectory(baseDirectory + OUTPUT_DIR);
			TestUtils.removeHDFSFiles(expectedFiles.toArray(new String[expectedFiles.size()]));
			TestUtils.clearDirectory(baseDirectory + EXPECTED_DIR);
			TestUtils.removeFiles(new String[] { sourceDirectory + selectedTest + ".dmlt" });
			TestUtils.removeFiles(new String[] { sourceDirectory + selectedTest + ".Rt" });
		}

		TestUtils.clearAssertionInformation();
	}

	/**
	 * Disables the deletion of files and directories in the output and expected
	 * folder for this test.
	 */
	public void disableOutAndExpectedDeletion() {
		setOutAndExpectedDeletionDisabled(true);
	}

	/**
	 * Enables detection of expected output of a line in standard output stream.
	 *
	 * @param expectedLine
	 */
	public void setExpectedStdOut(String expectedLine) {
		this.expectedStdOut = expectedLine;
		originalPrintStreamStd = System.out;
		iExpectedStdOutState = 1;
		System.setOut(new PrintStream(new ExpectedOutputStream()));
	}

	/**
	 * This class is used to compare the standard output stream against an
	 * expected string.
	 *
	 */
	class ExpectedOutputStream extends OutputStream {
		private String line = "";

		@Override
		public void write(int b) throws IOException {
			line += String.valueOf((char) b);
			if (((char) b) == '\n') {
				/** new line */
				if (line.contains(expectedStdOut)) {
					iExpectedStdOutState = 2;
					System.setOut(originalPrintStreamStd);
				} else {
					// Reset buffer
					line = "";
				}
			}
			originalPrintStreamStd.write(b);
		}
	}

	public void setExpectedStdErr(String expectedLine) {
		this.expectedStdErr = expectedLine;
		originalErrStreamStd = System.err;
		iExpectedStdErrState = 1;
		System.setErr(new PrintStream(new ExpectedErrorStream()));
	}

	/**
	 * This class is used to compare the standard error stream against an
	 * expected string.
	 *
	 */
	class ExpectedErrorStream extends OutputStream {
		private String line = "";

		@Override
		public void write(int b) throws IOException {
			line += String.valueOf((char) b);
			if (((char) b) == '\n') {
				/** new line */
				if (line.contains(expectedStdErr)) {
					iExpectedStdErrState = 2;
					System.setErr(originalErrStreamStd);
				} else {
					// Reset buffer
					line = "";
				}
			}
			originalErrStreamStd.write(b);
		}
	}

	/**
	 * Enables detection of unexpected output of a line in standard output stream.
	 *
	 * @param unexpectedLine  String that should not occur in stdout.
	 */
	public void setUnexpectedStdOut(String unexpectedLine) {
		this.unexpectedStdOut = unexpectedLine;
		originalPrintStreamStd = System.out;
		System.setOut(new PrintStream(new UnexpectedOutputStream()));
	}

	/**
	 * This class is used to compare the standard output stream against
	 * an unexpected string.
	 */
	class UnexpectedOutputStream extends OutputStream {
		private String line = "";

		@Override
		public void write(int b) throws IOException {
			line += String.valueOf((char) b);
			if (((char) b) == '\n') {
				/** new line */
				if (line.contains(unexpectedStdOut)) {
					iUnexpectedStdOutState = 1;  // error!
				} else {
					line = "";  // reset buffer
				}
			}
			originalPrintStreamStd.write(b);
		}
	}

	/**
	 * <p>
	 * Generates a matrix containing easy to debug values in its cells.
	 * </p>
	 *
	 * @param rows
	 * @param cols
	 * @param bContainsZeros
	 *            If true, the matrix contains zeros. If false, the matrix
	 *            contains only positive values.
	 * @return
	 */
	protected double[][] createNonRandomMatrixValues(int rows, int cols, boolean bContainsZeros) {
		return TestUtils.createNonRandomMatrixValues(rows, cols, bContainsZeros);
	}

	/**
	 * <p>
	 * Generates a matrix containing easy to debug values in its cells. The
	 * generated matrix contains zero values
	 * </p>
	 *
	 * @param rows
	 * @param cols
	 * @return
	 */
	protected double[][] createNonRandomMatrixValues(int rows, int cols) {
		return TestUtils.createNonRandomMatrixValues(rows, cols, true);
	}

	/**
	 * @return TRUE if the test harness is not deleting temporary files
	 */
	protected boolean isOutAndExpectedDeletionDisabled() {
		return isOutAndExpectedDeletionDisabled;
	}

	/**
	 * Call this method from a subclass's setUp() method.
	 * @param isOutAndExpectedDeletionDisabled
	 *            TRUE to disable code that deletes temporary files for this
	 *            test case
	 */
	protected void setOutAndExpectedDeletionDisabled(
			boolean isOutAndExpectedDeletionDisabled) {
		this.isOutAndExpectedDeletionDisabled = isOutAndExpectedDeletionDisabled;
	}

	protected String input(String input) {
		return baseDirectory + INPUT_DIR + input;
	}

	protected String inputDir() {
		return baseDirectory + INPUT_DIR;
	}

	protected String output(String output) {
		return baseDirectory + OUTPUT_DIR + output;
	}

	protected String outputDir() {
		return baseDirectory + OUTPUT_DIR;
	}

	protected String expected(String expected) {
		return baseDirectory + EXPECTED_DIR + cacheDir + expected;
	}

	protected String expectedDir() {
		return baseDirectory + EXPECTED_DIR + cacheDir;
	}

	protected String getScript() {
		return sourceDirectory + selectedTest + ".dml";
	}

	protected String getRScript() {
		return sourceDirectory + selectedTest + ".R";
	}

	protected String getRCmd(String ... args) {
		StringBuilder sb = new StringBuilder();
		sb.append("Rscript ");
		sb.append(getRScript());
		for (String arg : args) {
			sb.append(" ");
			sb.append(arg);
		}
		return sb.toString();
	}

	private boolean isTargetTestDirectory(String path) {
		return (path != null && path.contains(getClass().getSimpleName()));
	}

	private void setCacheDirectory(String directory) {
		cacheDir = (directory != null) ? directory : "";
		if (cacheDir.length() > 0 && !cacheDir.endsWith("/")) {
			cacheDir += "/";
		}
	}

	private static String getSourceDirectory(String testDirectory) {
		String sourceDirectory = "";
		if (null != testDirectory) {
			if (testDirectory.endsWith("/"))
				testDirectory = testDirectory.substring(0, testDirectory.length() - "/".length());
			sourceDirectory = testDirectory.substring(0, testDirectory.lastIndexOf("/") + "/".length());
		}
		return sourceDirectory;
	}

	/**
	 * <p>
	 * Adds a frame to the input path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param data
	 *            two dimensional frame data
	 * @param bIncludeR
	 *            generates also the corresponding R frame data
	 * @throws IOException
	 */
	protected double[][] writeInputFrame(String name, double[][] data, boolean bIncludeR, ValueType[] schema, OutputInfo oi) throws IOException {
		String completePath = baseDirectory + INPUT_DIR + name;
		String completeRPath = baseDirectory + INPUT_DIR + name + ".csv";

		try {
			cleanupExistingData(baseDirectory + INPUT_DIR + name, bIncludeR);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		TestUtils.writeTestFrame(completePath, data, schema, oi);
		if (bIncludeR) {
			TestUtils.writeTestFrame(completeRPath, data, schema, OutputInfo.CSVOutputInfo, true);
			inputRFiles.add(completeRPath);
		}
		if (DEBUG)
			TestUtils.writeTestFrame(DEBUG_TEMP_DIR + completePath, data, schema, oi);
		inputDirectories.add(baseDirectory + INPUT_DIR + name);

		return data;
	}

	protected double[][] writeInputFrameWithMTD(String name, double[][] data, boolean bIncludeR, ValueType[] schema, OutputInfo oi) throws IOException {
		MatrixCharacteristics mc = new MatrixCharacteristics(data.length, data[0].length, OptimizerUtils.DEFAULT_BLOCKSIZE, data[0].length, -1);
		return writeInputFrameWithMTD(name, data, bIncludeR, mc, schema, oi);
	}

	protected double[][] writeInputFrameWithMTD(String name, double[][] data, boolean bIncludeR, MatrixCharacteristics mc, ValueType[] schema, OutputInfo oi) throws IOException {
		writeInputFrame(name, data, bIncludeR, schema, oi);

		// write metadata file
		try
		{
			String completeMTDPath = baseDirectory + INPUT_DIR + name + ".mtd";
			HDFSTool.writeMetaDataFile(completeMTDPath, null, schema, DataType.FRAME, mc, oi);
		}
		catch(IOException e)
		{
			e.printStackTrace();
			throw new RuntimeException(e);
		}

		return data;
	}

	/**
	 * <p>
	 * Adds a frame to the input path and writes it to a file.
	 * </p>
	 *
	 * @param name
	 *            directory name
	 * @param data
	 *            two dimensional frame data
	 * @param schema
	 * @param oi
	 * @throws IOException
	 */
	protected double[][] writeInputFrame(String name, double[][] data, ValueType[] schema, OutputInfo oi)
			throws IOException
	{
		return writeInputFrame(name, data, false, schema, oi);
	}

	protected boolean heavyHittersContainsString(String... str) {
		for( String opcode : Statistics.getCPHeavyHitterOpCodes())
			for( String s : str )
				if(opcode.equals(s))
					return true;
		return false;
	}
	
	protected boolean heavyHittersContainsString(String str, int minCount) {
		int count = 0;
		for( String opcode : Statistics.getCPHeavyHitterOpCodes())
			count += opcode.equals(str) ? 1 : 0;
		return (count >= minCount);
	}
	
	protected boolean heavyHittersContainsSubString(String... str) {
		for( String opcode : Statistics.getCPHeavyHitterOpCodes())
			for( String s : str )
				if(opcode.contains(s))
					return true;
		return false;
	}
	
	protected boolean heavyHittersContainsSubString(String str, int minCount) {
		int count = 0;
		for( String opcode : Statistics.getCPHeavyHitterOpCodes())
			count += opcode.contains(str) ? 1 : 0;
		return (count >= minCount);
	}

	/**
	 * Create a SystemDS-preferred Spark Session.
	 *
	 * @param appName the application name
	 * @param master the master value (ie, "local", etc)
	 * @return Spark Session
	 */
	public static SparkSession createSystemDSSparkSession(String appName, String master) {
		Builder builder = SparkSession.builder();
		if (appName != null) {
			builder.appName(appName);
		}
		if (master != null) {
			builder.master(master);
		}
		builder.config("spark.driver.maxResultSize", "0");
		if (SparkExecutionContext.FAIR_SCHEDULER_MODE) {
			builder.config("spark.scheduler.mode", "FAIR");
		}
		builder.config("spark.locality.wait", "5s");
		SparkSession spark = builder.getOrCreate();
		return spark;
	}

	public static String getMatrixAsString(double[][] matrix) {
		try {
			return DataConverter.toString(DataConverter.convertToMatrixBlock(matrix));
		} catch (DMLRuntimeException e) {
			return "N/A";
		}
	}
}
