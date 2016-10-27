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
        public BPlusTree<TK, TV> bBplusTree;

        public Node<TK, TV> Root
        {
            get { return bBplusTree.Root; }
            set { bBplusTree.Root = value; }
        }

        public void Delete(TK key)
        {
            bBplusTree.Delete(key);
            bBplusTree.RepairAfterDelete();
        }

        public void Insert(TV value)
        {
            bBplusTree.Insert(value);
            bBplusTree.InsertRepair();
        }

        public void Search(TK key)
        {
            bBplusTree.Search(key);
        }

        public void Traverse(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            bBplusTree.Traverse(node,action);
        }

        public Node<TK, TV> SearchInTimes(int times, TV value, List<TV> ridList)
        {
            return bBplusTree.SearchInTimes(times, value, ridList);
        }

        // TODO
        public void Reset(TV value)
        {
            
        }

        // TODO 单例
        public BPlusTreeProvider(int degree)
        {
            bBplusTree = new BPlusTree<TK, TV>(degree);
        }

        public void InsertRepair(Node<TK, TV> node)
        {
            bBplusTree.InsertRepair(node);
        }
    }
}
