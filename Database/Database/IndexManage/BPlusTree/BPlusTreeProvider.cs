using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class BPlusTreeProvider<TK, TV> : Iprovider<TK, TV> 
        where TV : INode<TK>
        where TK : IComparable<TK>
    {

        static BPlusTree<TK, TV> bBplusTree;

        public Node<TK, TV> Root
        {
            get { return bBplusTree.Root; }
        }

        public static BPlusTree<TK, TV> GetBBplusTree(int degree)
        {
            if (bBplusTree == null)
            {
                bBplusTree = new BPlusTree<TK, TV>(degree);
            }
            else if (bBplusTree.Degree != degree) throw new Exception();

            return bBplusTree;
        }

        public void Delete(TK key)
        {
            bBplusTree.Delete(key);
        }

        public void Insert(TV value, Action<Node<TK, TV>> func)
        {
            bBplusTree.Insert(value,func);
        }

        public void Search(TK key)
        {
            bBplusTree.Search(key);
        }

        public void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            bBplusTree.Traverse(node,action);
        }

        // TODO
        public void Reset(TV value)
        {
            
        }

        private BPlusTreeProvider()
        {
        }
    }
}
