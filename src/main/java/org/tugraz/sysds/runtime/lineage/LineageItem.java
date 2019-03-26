package org.tugraz.sysds.runtime.lineage;

import java.util.ArrayList;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

public class LineageItem {

    private final CPOperand _variable;
    private final ArrayList<LineageItem> _lineages;

    public LineageItem(CPOperand variable) {
        _variable = variable;
        _lineages = null;
    }

    public LineageItem(CPOperand variable, ArrayList<LineageItem> lineages) {
        super();
        _variable = variable;
        // TODO bnyra: do i need here the new keyword?
        _lineages = new ArrayList<>(lineages);
    }

    public CPOperand getVariable() {
        return this._variable;
    }

    public ArrayList<LineageItem> getLineages() {
        return this._lineages;
    }

    public void print() {
        System.out.println("Write Lineage Trace of " + this.getVariable().getName());
        this.print("--");
    }

    public void print(String prefix) {
        if (this._lineages == null)
            return;

        for (LineageItem li : this._lineages) {
            System.out.println(prefix + li.getVariable().getName());
            li.print(prefix + "--");
        }
    }
}
