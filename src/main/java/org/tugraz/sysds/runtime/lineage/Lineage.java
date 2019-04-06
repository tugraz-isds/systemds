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
                    createVariableInstruction(inst);
                    break;
                }
                case RemoveVariable: {
                    removeInstruction(inst);
                    break;
                }
                case AssignVariable:
                case CopyVariable: {
                    copyInstruction(inst);
                    break;
                }
                case Write: {
                    writeInstruction(inst, ec);
                    break;
                }
                case MoveVariable: {
                    moveInstruction(inst);
                    break;
                }
                default:
                    System.out.printf("Oh no! Unknown VariableCPInstruction traced: %s\n", instruction.getOpcode());
                    break;
            }
        } else if (instruction instanceof LineageTraceable) {
            addLineageItem(((LineageTraceable) instruction).getLineageItem());
        } else {
            System.out.printf("Oh no! Unknown Instruction traced: %s (%s)\n", instruction.getOpcode(), instruction.getClass().getName());
        }
    }

    public static void removeLineageItem(String key) {
        LineageItem li = lineage_traces.get(key);
        if (li == null)
            return;

        removeAncestorLink(li);
        lineage_traces.remove(key);
    }

    public static void addLineageItem(LineageItem li) {
        if (lineage_traces.get(li.getKey()) != null) {
            removeAncestorLink(lineage_traces.get(li.getKey()));
            lineage_traces.remove(li.getKey());
        }
        lineage_traces.put(li.getKey(), li);
    }

    public static LineageItem getOrCreate(CPOperand variable) {
        if (variable == null)
            return null;
        if (lineage_traces.get(variable.getName()) == null)
            return new LineageItem(variable);
        return lineage_traces.get(variable.getName());
    }

    private static void writeInstruction(VariableCPInstruction inst, ExecutionContext ec) {
        LineageItem li = lineage_traces.get(inst.getInput1().getName());
        String desc = Explain.explain(li);

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
    }

    private static void removeInstruction(VariableCPInstruction inst) {
        removeLineageItem(inst.getInput1().getName());
    }

    private static void moveInstruction(VariableCPInstruction inst) {
        if (inst.getInput2().getName().equals("__pred")) {
            removeLineageItem(inst.getInput1().getName());
        } else {
            LineageItem li_source = get(inst.getInput1());
            LineageItem li;
            ArrayList<LineageItem> lineages = new ArrayList<>();
            if (li_source != null) {
                lineages.add(li_source);
                li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
            } else {
                lineages.add(getOrCreate(inst.getInput1()));
                if (inst.getInput3() != null)
                    lineages.add(getOrCreate(inst.getInput3()));
                li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
            }
            addLineageItem(li);
        }
    }

    private static void copyInstruction(VariableCPInstruction inst) {
        LineageItem li_source = get(inst.getInput1());
        LineageItem li;
        ArrayList<LineageItem> lineages = new ArrayList<>();
        if (li_source != null) {
            lineages.add(li_source);
            li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
        } else {
            lineages.add(getOrCreate(inst.getInput1()));
            li = new LineageItem(inst.getInput2(), lineages, inst.getOpcode());
        }
        addLineageItem(li);
    }

    private static void createVariableInstruction(VariableCPInstruction inst) {
        ArrayList<LineageItem> lineages = new ArrayList<>();
        lineages.add(lineage_traces.getOrDefault(inst.getInput2(),
                new LineageItem(inst.getInput2())));
        lineages.add(lineage_traces.getOrDefault(inst.getInput3(),
                new LineageItem(inst.getInput3())));
        LineageItem li = new LineageItem(inst.getInput1(), lineages, inst.getOpcode());
        addLineageItem(li);
    }

    public static LineageItem get(CPOperand variable) {
        if (variable == null)
            return null;
        return lineage_traces.get(variable.getName());
    }

    private static void removeAncestorLink(LineageItem li) {
        if (li.getChildren().isEmpty()) {
            ArrayList<LineageItem> ancestors = new ArrayList<>(li.getAncestors());
            for (LineageItem an : ancestors) {
                li.getAncestors().remove(an);
                an.getChildren().remove(li);
                removeAncestorLink(an);
            }
        }
    }
}
