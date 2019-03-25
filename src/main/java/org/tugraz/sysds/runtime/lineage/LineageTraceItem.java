package org.tugraz.sysds.runtime.lineage;

import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

import java.util.HashSet;

public class LineageTraceItem {

    public LineageTraceItem(CPOperand variable) {
        _variable = variable;
        _lineages = null;
    }

    public LineageTraceItem(CPOperand variable, HashSet<LineageTraceItem> lineages) {
        super();
        _variable = variable;
        _lineages = new HashSet<>(lineages);
    }

    public CPOperand getVariable(){
        return this._variable;
    }

    public void print(){
        System.out.println("Write Lineage Trace of " + this.getVariable().getName());
        this.print("--");
    }
    public void print(String prefix){
        if (this._lineages == null)
            return;

        for (LineageTraceItem lti: this._lineages) {
            System.out.println(prefix + lti.getVariable().getName());
            lti.print(prefix + "--");
        }
    }

    // Frame related members
    private final CPOperand _variable;
    private final HashSet<LineageTraceItem> _lineages;

}
