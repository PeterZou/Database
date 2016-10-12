using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class Node<T>
    {
        public bool IsLeaf { set; get; }
        public Node<T> Parent { set; get; }
        public List<T> Keys { set; get; }
        public List<Node<T>> ChildrenNodes { set; get; }

        public Node(bool isLeaf, Node<T> parent, T value)
        {
            this.IsLeaf = isLeaf;
            this.Parent = parent;
            Keys = new List<T>();
            Keys.Add(value);
        }
    }
}
