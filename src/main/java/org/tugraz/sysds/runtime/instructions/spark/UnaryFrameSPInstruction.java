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



package org.tugraz.sysds.runtime.instructions.spark;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.controlprogram.context.SparkExecutionContext;
import org.tugraz.sysds.runtime.instructions.InstructionUtils;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.matrix.data.FrameBlock;
import org.tugraz.sysds.runtime.matrix.operators.Operator;
import org.tugraz.sysds.runtime.meta.DataCharacteristics;


public class UnaryFrameSPInstruction extends UnarySPInstruction {
    protected UnaryFrameSPInstruction(Operator op, CPOperand in, CPOperand out, String opcode, String instr) {
        super(SPInstruction.SPType.Unary, op, in, out, opcode, instr);
    }

    public static UnaryFrameSPInstruction parseInstruction (String str ) {
        CPOperand in = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
        CPOperand out = new CPOperand("", Types.ValueType.UNKNOWN, Types.DataType.UNKNOWN);
        String opcode = parseUnaryInstruction(str, in, out);
        return new UnaryFrameSPInstruction(InstructionUtils.parseUnaryOperator(opcode), in, out, opcode, str);
    }

    @Override
    public void processInstruction(ExecutionContext ec) {
        SparkExecutionContext sec = (SparkExecutionContext)ec;

        //get input
        JavaPairRDD<Long, FrameBlock> in = sec.getFrameBinaryBlockRDDHandleForVariable(input1.getName() );
        DataCharacteristics dcIn = sec.getDataCharacteristics(input1.getName());
        DataCharacteristics dcOut = sec.getDataCharacteristics(output.getName());

        System.out.println("dcout rows before"+dcOut.getRows());
        System.out.println("dcout cols before"+dcOut.getCols());
        System.out.println("dcout block before"+dcOut.getBlocksize());

        //  checkValidOutputDimensions(dcOut);
        dcOut.set(1, dcIn.getCols(), dcIn.getBlocksize());
        JavaPairRDD<Long,FrameBlock> out = in.mapValues(new UnaryFrameSPInstruction.RDDFrameBuiltinUnaryOp());
        //out.reduce();
        dcOut = sec.getDataCharacteristics(output.getName());


        sec.setRDDHandleForVariable(output.getName(), out);
        sec.addLineageRDD(output.getName(), input1.getName());

    }
    private static class RDDFrameBuiltinUnaryOp implements Function<FrameBlock, FrameBlock>
    {
        private static final long serialVersionUID = -3128192099832877492L;

        @Override
        public FrameBlock call(FrameBlock aLong) throws Exception {
            System.out.println();
            return (FrameBlock) aLong.detectSchemaFromRow();
        }


    }

}
