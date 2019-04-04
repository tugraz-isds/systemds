package org.tugraz.sysds.runtime.lineage;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.tugraz.sysds.lops.Lop;
import org.tugraz.sysds.runtime.instructions.cp.CPOperand;

public class LineageItem {
    private static int _id_counter = 1;

    private final int _id;
    private final String _opcode;
    private final CPOperand _variable;
    private final ArrayList<LineageItem> _lineages;

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
        // TODO bnyra: do i need here the new keyword?
        _lineages = new ArrayList<>(lineages);
    }

    public CPOperand getVariable() {
        return this._variable;
    }

    public ArrayList<LineageItem> getLineages() {
        return this._lineages;
    }

//    public void setOpcode(String opcode){
//        this._opcode = opcode;
//    }
//
//    public void setLineages(ArrayList<LineageItem> lineages) {
//        this._lineages.clear();
//        this._lineages.addAll(lineages);
//    }

    public int getId() {
        return this._id;
    }


    public void print() {
        System.out.println(this.toString());
        if (this._lineages != null)
            for (LineageItem li : this._lineages)
                li.print();
    }

    @Override
    public String toString() {
        if (!this._opcode.isEmpty()) {
            String ids = this._lineages.stream()
                    .map(i -> String.format("(%d)", i.getId()))
                    .collect(Collectors.joining(" "));
            return String.format("(%d) %s %s", this.getId(), this._opcode, ids);
        } else
            return String.format("(%d) %s", this.getId(), this.getVariable().getName());
    }
   
    private static int getUniqueId() {
        int id = _id_counter;
        _id_counter++;
        return id;
    }
}
