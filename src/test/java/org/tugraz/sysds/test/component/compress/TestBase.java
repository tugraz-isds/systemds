
package org.tugraz.sysds.test.component.compress;

import org.tugraz.sysds.runtime.compress.CompressedMatrixBlock;
import org.tugraz.sysds.runtime.matrix.data.MatrixBlock;
import org.tugraz.sysds.runtime.util.DataConverter;
import org.tugraz.sysds.test.AutomatedTestBase;
import org.tugraz.sysds.test.TestUtils;
import org.tugraz.sysds.test.TestConstants;
import org.tugraz.sysds.test.TestConstants.CompressionType;
import org.tugraz.sysds.test.TestConstants.MatrixType;
import org.tugraz.sysds.test.TestConstants.SparsityType;
import org.tugraz.sysds.test.TestConstants.ValueRange;
import org.tugraz.sysds.test.TestConstants.ValueType;

public class TestBase extends AutomatedTestBase {

	protected ValueType valType;
	protected ValueRange valRange;
	protected CompressionType compType;
	protected boolean compress;

	protected int rows;
	protected int cols;
	protected int min;
	protected int max;
	protected double samplingRatio;
	protected double sparsity;

	// Input
	protected double[][] input;
	protected MatrixBlock mb;

	public TestBase(SparsityType sparType, ValueType valType, ValueRange valueRange, CompressionType compType,
		MatrixType matrixType, boolean compress, double samplingRatio) {
		this.sparsity = TestConstants.getSparsityValue(sparType);
		this.rows = TestConstants.getNumberOfRows(matrixType);
		this.cols = TestConstants.getNumberOfColumns(matrixType);
		this.max = TestConstants.getMaxRangeValue(valueRange);
		if(valType == ValueType.CONST) {
			min = max;
		}
		else {
			min = TestConstants.getMinRangeValue(valueRange);
		}
		this.valRange = valueRange;
		this.valType = valType;
		this.compType = compType;
		this.compress = compress;

		this.samplingRatio = samplingRatio;

		input = TestUtils.generateTestMatrix(rows, cols, min, max, sparsity, 7);
		mb = getMatrixBlockInput(input);
	}

	protected MatrixBlock getMatrixBlockInput(double[][] input) {
		// generate input data

		if(valType == ValueType.RAND_ROUND_OLE || valType == ValueType.RAND_ROUND_DDC) {
			CompressedMatrixBlock.ALLOW_DDC_ENCODING = (valType == ValueType.RAND_ROUND_DDC);
			input = TestUtils.round(input);
		}

		return DataConverter.convertToMatrixBlock(input);
	}

	@Override
	public void setUp() {
	}

	@Override
	public void tearDown() {
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		builder.append("args: ");

		builder.append(String.format("%6s%14s", "Vt:", valType));
		builder.append(String.format("%6s%8s", "Vr:", valRange));
		builder.append(String.format("%6s%8s", "CP:", compType));
		builder.append(String.format("%6s%5s", "CD:", compress));
		builder.append(String.format("%6s%5s", "Rows:", rows));
		builder.append(String.format("%6s%5s", "Cols:", cols));
		builder.append(String.format("%6s%12s", "Min:", min));
		builder.append(String.format("%6s%12s", "Max:", max));
		builder.append(String.format("%6s%5s", "Spar:", sparsity));
		builder.append(String.format("%6s%4s", "Samp:", samplingRatio));

		return builder.toString();
	}

}