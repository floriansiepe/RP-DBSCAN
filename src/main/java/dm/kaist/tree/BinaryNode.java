package dm.kaist.tree;

import dm.kaist.graph.Edge;

import java.io.Serializable;

public class BinaryNode implements Serializable {
    public float weight;
    public Edge edge = null;
    BinaryNode parent = null;
    //large
    BinaryNode left = null;
    //ged
    BinaryNode right = null;

    public BinaryNode(BinaryNode parent, BinaryNode left, BinaryNode right, Edge edge, float weight) {
        this.parent = parent;
        this.left = left;
        this.right = right;
        this.edge = edge;
        this.weight = weight;
    }
}
