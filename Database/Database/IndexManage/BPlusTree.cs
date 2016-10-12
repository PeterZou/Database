using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class BPlusTree<TK,TV> where TK : IComparable<TK>
    {
        public int Degree { set; get; }
        public Node<TV> Root{ set; get; }

        public TV Search(TK key)
        {
            return default(TV);
        }

        public void Insert(TV value)
        {
            // Root is null
            if (Root == null)
            {
                Root = new Node<TV>(true, null,value);
            }

            Traverse(Root, TraverseOutput);
        }

        public void Delete(TK key)
        { }

        /// <summary>
        /// Go through the tree include the nodes and leaves
        /// BFS
        /// </summary>
        public void Traverse(Node<TV> node, Action<Node<TV>> action)
        {
            action(node);

            if (node.ChildrenNodes != null)
            {
                foreach (var n in node.ChildrenNodes)
                {
                    Traverse(n, action);
                }
            } 
        }

        private void TraverseOutput(Node<TV> node)
        {
            Console.WriteLine("----------------");
            foreach (var n in node.Keys)
            {
                Console.WriteLine("key is " + n);
            }
        }

        private void Split(Node<TV> node)
        {

        }
    }
}
