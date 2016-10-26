using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class Node<TK,TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        public int Height { get; set; }

        // once get, will not change
        public TV CurrentRID { get; set; }

        // Record RID only the leaf node has this property
        public List<TV> Property { get; set; }

        public bool IsLeaf { set; get; }
        public Node<TK, TV> Parent { set; get; }
        public List<TK> Values { set; get; }
        public List<Node<TK, TV>> ChildrenNodes { set; get; }

        public Node()
        { }

        // must be a leaf node or root node
        public Node(bool isLeaf, Node<TK, TV> parent, TV property,int height)
        {
            this.Height = height;
            this.Property = new List<TV>();
            this.Property.Add(property);
            this.IsLeaf = isLeaf;
            this.Parent = parent;
            Values = new List<TK>();
            Values.Add(property.Key);
        }

        // delegate to set the new RID
        public Node<TK, TV> SetNode(bool leafOrNot)
        {
            var node = new Node<TK, TV>();
            node.Height = Height;
            node.IsLeaf = IsLeaf;
            node.Parent = new Node<TK, TV>();
            node.Values = new List<TK>();
            node.ChildrenNodes = new List<Node<TK, TV>>();

            if (leafOrNot)
            {
                node.Property = new List<TV>();
                this.Property = Property;
            }

            return node;
        }
    }
}
