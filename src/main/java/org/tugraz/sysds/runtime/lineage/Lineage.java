package org.tugraz.sysds.runtime.lineage;

import org.apache.hadoop.hdfs.server.namenode.CachePool;
import org.tugraz.sysds.runtime.instructions.Instruction;
import org.tugraz.sysds.runtime.instructions.cp.BinaryScalarScalarCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;
import org.tugraz.sysds.runtime.instructions.cp.UnaryCPInstruction;
import org.tugraz.sysds.runtime.instructions.cp.VariableCPInstruction;

import java.util.HashMap;
import java.util.HashSet;

public class Lineage {
    // static variable single_instance of type Singleton
//    private static Lineage single_instance = null;

    // private constructor restricted to this class itself
    private Lineage() {
    }

    private static HashMap<String, LineageTraceItem> lineage_traces = new HashMap<>();

    public static void trace(Instruction instruction) {
//        System.out.printf("Instruction: %s (%s)\n", instruction.getOpcode(), instruction.getClass().getName());

        if (instruction instanceof VariableCPInstruction && ((VariableCPInstruction) instruction).isCreateVariable()) {
            HashSet<LineageTraceItem> lineages = new HashSet<>();
            lineages.add(lineage_traces.getOrDefault(((VariableCPInstruction) instruction).getInput2(),
                    new LineageTraceItem(((VariableCPInstruction) instruction).getInput2())));
            lineages.add(lineage_traces.getOrDefault(((VariableCPInstruction) instruction).getInput3(),
                    new LineageTraceItem(((VariableCPInstruction) instruction).getInput3())));
            LineageTraceItem lti = new LineageTraceItem(((VariableCPInstruction) instruction).getInput1(), lineages);
            lineage_traces.put(lti.getVariable().getName(), lti);

        } else if (instruction instanceof VariableCPInstruction && ((VariableCPInstruction) instruction).isRemoveVariable()) {
            lineage_traces.remove(((VariableCPInstruction) instruction).getInput1().getName());

        } else if (instruction instanceof VariableCPInstruction && (((VariableCPInstruction) instruction).isAssignVariable() ||
                ((VariableCPInstruction) instruction).isCopyVariable())) {
            HashSet<LineageTraceItem> lineages = new HashSet<>();
            lineages.add(getOrCreate(((VariableCPInstruction) instruction).getInput1()));
            LineageTraceItem lti = new LineageTraceItem(((VariableCPInstruction) instruction).getInput2(), lineages);
            lineage_traces.put(lti.getVariable().getName(), lti);

        } else if (instruction instanceof VariableCPInstruction && ((VariableCPInstruction) instruction).isWriteVariable()) {
            LineageTraceItem lti = lineage_traces.get(((VariableCPInstruction) instruction).getInput1().getName());
            lti.print();

        } else if (instruction instanceof LineageTracable) {
            LineageTraceItem lti = ((LineageTracable) instruction).getLineageTraceItem();
            lineage_traces.put(lti.getVariable().getName(), lti);

        } else {
            System.out.printf("Oh no! Unknown instruction traced: %s (%s)\n", instruction.getOpcode(), instruction.getClass().getName());
        }
    }

    public static LineageTraceItem getOrCreate(CPOperand variable) {
        if (variable == null)
            return null;
        if (lineage_traces.get(variable.getName()) == null)
            return new LineageTraceItem(variable);
        return lineage_traces.get(variable.getName());
    }

    public static LineageTraceItem get(CPOperand variable) {
        if (variable == null)
            return null;
        return lineage_traces.get(variable.getName());
    }

//    // static method to create instance of Singleton class
//    public static Lineage getInstance() {
//        if (single_instance == null)
//            single_instance = new Lineage();
//
//        return single_instance;
//    }
}
