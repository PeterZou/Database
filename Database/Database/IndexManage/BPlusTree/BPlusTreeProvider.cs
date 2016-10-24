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

        public static BPlusTree<TK, TV> GetBBplusTree(int degree)
        {
            if (bBplusTree == null)
            {
                bBplusTree = new BPlusTree<TK, TV>(degree);
            }
            else if (bBplusTree.Degree != degree) throw new Exception();

            return bBplusTree;
        }

        Node<TK, TV> Reset(TV value)
        {

            return bBplusTree.Root;
        }

        public void Delete(TK key)
        {
            bBplusTree.Delete(key);
        }

        public void Insert(TV value)
        {
            bBplusTree.Insert(value);
        }

        public void Search(TK key)
        {
            bBplusTree.Search(key);
        }

        public void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            bBplusTree.Traverse(node,action);
        }

        private BPlusTreeProvider()
        {
        }
    }
}
