package org.tugraz.sysds.runtime.lineage;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.tugraz.sysds.common.Types;
import org.tugraz.sysds.runtime.DMLRuntimeException;
import org.tugraz.sysds.runtime.controlprogram.context.ExecutionContext;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.VariableCPInstruction;
import org.tugraz.sysds.runtime.io.IOUtilFunctions;
import org.tugraz.sysds.runtime.util.HDFSTool;
import org.tugraz.sysds.utils.Explain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class Lineage {

    private static HashMap<String, LineageItem> lineage_traces = new HashMap<>();

    private Lineage() {
    }

    public static void trace(Instruction instruction, ExecutionContext ec) {
        if (instruction instanceof VariableCPInstruction) {
            VariableCPInstruction inst = ((VariableCPInstruction) instruction);
            switch (inst.getVariableOpcode()) {
                case CreateVariable: {
                    createVariableLineageItem(inst);
                    break;
                }
                case RemoveVariable: {
                    removeLineageItem(inst);
                    break;
                }
                case AssignVariable:
                case CopyVariable: {
                    copyVariableLineage(inst);
                    break;
                }
                case Write: {
                    writeLineageItem(inst, ec);
                    break;
                }
                case MoveVariable: {
                    moveLineageItem(inst);
                    break;
                }
                default:
                    System.out.printf("Oh no! Unknown VariableCPInstruction traced: %s\n", instruction.getOpcode());
                    break;
            }
        } else if (instruction instanceof LineageTraceable) {
            LineageItem li = ((LineageTraceable) instruction).getLineageItem();
            lineage_traces.put(li.getVariable().getName(), li);
        } else {
            System.out.printf("Oh no! Unknown Instruction traced: %s (%s)\n", instruction.getOpcode(), instruction.getClass().getName());
        }
    }

    private static void moveLineageItem(VariableCPInstruction inst) {
        // TODO bnyra: What is a mov instruction doing?
        LineageItem li_source = get(inst.getInput1());
        if (li_source != null) {
            LineageItem li = new LineageItem(inst.getInput2(), li_source.getLineages(), inst.getOpcode());
            lineage_traces.put(li.getVariable().getName(), li);
        } else {
            ArrayList<LineageItem> lineages = new ArrayList<>();

            if (inst.getInput1() != null)
                lineages.add(getOrCreate(inst.getInput1()));
            if (inst.getInput3() != null)
                lineages.add(getOrCreate(inst.getInput3()));

            LineageItem li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
            lineage_traces.put(li.getVariable().getName(), li);
        }
    }

    private static void writeLineageItem(VariableCPInstruction inst, ExecutionContext ec) {
        LineageItem li = lineage_traces.get(inst.getInput1().getName());
        String desc = Explain.explainLineageItem(li);

        String fname = ec.getScalarInput(inst.getInput2().getName(), Types.ValueType.STRING, inst.getInput2().isLiteral()).getStringValue();
        fname += ".lineage";

        try {
            HDFSTool.writeStringToHDFS(desc, fname);
            FileSystem fs = IOUtilFunctions.getFileSystem(fname);
            if (fs instanceof LocalFileSystem) {
                Path path = new Path(fname);
                IOUtilFunctions.deleteCrcFilesFromLocalFileSystem(fs, path);
            }

        } catch (IOException e) {
            throw new DMLRuntimeException(e);
        }

//        System.out.printf("Write Lineage Trace: (%d) %s\n", li.getId(), li.getVariable().getName());
//        System.out.print(desc);
    }

    private static void copyVariableLineage(VariableCPInstruction inst) {
        LineageItem li_source = get(inst.getInput1());
        LineageItem li;
        if (li_source != null)
            li = new LineageItem(inst.getInput2(), li_source.getLineages(), inst.getOpcode());
        else {
            ArrayList<LineageItem> lineages = new ArrayList<>();
            lineages.add(getOrCreate(inst.getInput1()));
            li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
        }
        lineage_traces.put(li.getVariable().getName(), li);
    }

    private static void removeLineageItem(VariableCPInstruction inst) {
        // TODO bnyra: Cleanup for removing, maybe the same like __pred
        lineage_traces.remove(inst.getInput1().getName());
    }

    private static void createVariableLineageItem(VariableCPInstruction inst) {
        ArrayList<LineageItem> lineages = new ArrayList<>();
        lineages.add(lineage_traces.getOrDefault(inst.getInput2(),
                new LineageItem(inst.getInput2())));
        lineages.add(lineage_traces.getOrDefault(inst.getInput3(),
                new LineageItem(inst.getInput3())));
        LineageItem li = new LineageItem(inst.getInput1(), lineages, inst.getOpcode());
        lineage_traces.put(li.getVariable().getName(), li);
    }

    public static LineageItem getOrCreate(CPOperand variable) {
        if (variable == null)
            return null;
        if (lineage_traces.get(variable.getName()) == null)
            return new LineageItem(variable);
        return lineage_traces.get(variable.getName());
    }

    public static LineageItem get(CPOperand variable) {
        if (variable == null)
            return null;
        return lineage_traces.get(variable.getName());
    }
}
