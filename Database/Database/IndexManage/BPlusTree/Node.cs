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

        #region leaf node
        public TV NextNode { get; set; }
        public TV PreviousNode { get; set; }
        #endregion

        public Node()
        {
            this.IsLeaf = true;
            this.Parent = null;
            this.Height = 0;
            this.Property = new List<TV>();
            this.Values = new List<TK>();
            this.NextNode = null;
            this.PreviousNode = null;
        }

        // must be a leaf node or root node
        public Node(bool isLeaf, Node<TK, TV> parent, TV property,int height)
        {
            this.Height = height;
            this.Property = new List<TV>();
            this.Property.Add(property);
            this.IsLeaf = isLeaf;
            this.Parent = parent;
            this.Values = new List<TK>();
            this.Values.Add(property.Key);
            this.NextNode = NextNode;
            this.PreviousNode = PreviousNode;
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

        // Just for IO
        public Node<TK, TV> SetNode()
        {
            var node = new Node<TK, TV>();
            node.IsLeaf = IsLeaf;
            node.CurrentRID = CurrentRID;

            return node;
        }
    }
}
