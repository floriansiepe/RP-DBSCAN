package dm.kaist.meta;

import dm.kaist.io.Point;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Cell implements Serializable {

    public long cellId = Long.MAX_VALUE;
    public List<Integer> cellCoords = null;
    public List<Point> pts = null;

    public Cell(List<Integer> cellCoords) {
        this.cellCoords = cellCoords;
        pts = new ArrayList<Point>();
    }

    public Cell(List<Integer> cellCoords, List<Point> pts) {
        this.cellCoords = cellCoords;
        this.pts = pts;
    }

    public Cell(List<Integer> cellCoords, Iterable<Point> pts) {
        this.cellCoords = cellCoords;
        this.pts = new ArrayList<Point>();

        for (Point pt : pts)
            this.pts.add(pt);
    }


    public void addPoint(Point pt) {
        if (cellId > (int) pt.id)
            cellId = (int) pt.id;
        pts.add(pt);
    }

    public void addAll(Cell other) {
        for (Point pt : other.pts)
            this.addPoint(pt);
    }


    public int getCount() {
        return this.pts.size();
    }

    @Override
    public boolean equals(Object obj) {
        // TODO Auto-generated method stub
        Cell objCell = (Cell) obj;

        return cellId == (objCell.cellId);

    }

    @Override
    public int hashCode() {
        // TODO Auto-generated method stub
        return Long.valueOf(cellId).hashCode();
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return Long.valueOf(cellId).toString();
    }
}
