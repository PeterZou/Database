using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class Node<TK,TV> 
    {
        public bool IsLeaf { set; get; }
        public Node<TK, TV> Parent { set; get; }
        public List<TV> Values { set; get; }
        public List<Node<TK, TV>> ChildrenNodes { set; get; }

        public Node()
        { }

        // must be a leaf node or root node
        public Node(bool isLeaf, Node<TK, TV> parent, TV value)
        {
            this.IsLeaf = isLeaf;
            this.Parent = parent;
            Values = new List<TV>();
            Values.Add(value);
        }

        public Node<TK, TV> SetNode()
        {
            var node = new Node<TK, TV>();
            node.IsLeaf = IsLeaf;
            node.Parent = new Node<TK, TV>();
            node.Values = new List<TV>();
            node.ChildrenNodes = new List<Node<TK, TV>>();

            return node;
        }
    }
}
