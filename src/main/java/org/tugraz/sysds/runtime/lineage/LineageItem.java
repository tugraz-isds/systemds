package org.tugraz.sysds.runtime.lineage;

import java.util.ArrayList;

import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

public class LineageItem {
    private static int _id_counter = 1;

    private final int _id;
    private final String _opcode;
    private final CPOperand _variable;
    private final ArrayList<LineageItem> _lineages;
    private boolean _visited = false;

    public LineageItem(CPOperand variable) {
        _id = LineageItem.getUniqueId();
        _variable = new CPOperand(variable);
        _opcode = "";
        _lineages = null;
    }

    public LineageItem(CPOperand variable, ArrayList<LineageItem> lineages, String opcode) {
        super();
        _id = LineageItem.getUniqueId();
        _variable = variable;
        _opcode = opcode;
        _lineages = new ArrayList<>(lineages);
    }

    public CPOperand getVariable() {
        return this._variable;
    }

    public ArrayList<LineageItem> getLineages() {
        return this._lineages;
    }

    public String getKey() {
        return this._variable.getName();
    }

    public boolean isVisited() {
        return _visited;
    }

    public void setVisited() {
        setVisited(true);
    }

    public void setVisited(boolean flag) {
        _visited = flag;
    }

    public int getId() {
        return this._id;
    }

    public String getOpcode() {
        return this._opcode;
    }

    public LineageItem resetVisitStatus() {
        if (!isVisited())
            return this;
        for (LineageItem li : getLineages())
            li.resetVisitStatus();
        setVisited(false);
        return this;
    }

    public static void resetVisitStatus(ArrayList<LineageItem> lis) {
        if (lis != null)
            for (LineageItem liRoot : lis)
                liRoot.resetVisitStatus();
    }

    private static int getUniqueId() {
        int id = _id_counter;
        _id_counter++;
        return id;
    }
}
