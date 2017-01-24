using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage.BPlusTree
{
    public class BPlusTreeProvider<TK, TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        public BPlusTree<TK, TV> bBplusTree;

        public Node<TK, TV> SearchNode
        {
            get { return bBplusTree.SearchNode; }
            set { bBplusTree.SearchNode = value; }
        }

        public Node<TK, TV> Root
        {
            get { return bBplusTree.Root; }
            set { bBplusTree.Root = value; }
        }

        public void Delete(TK key)
        {
            bBplusTree.Delete(key);
            //bBplusTree.RepairAfterDelete();
        }

        public void Insert(TV value)
        {
            bBplusTree.Insert(value);
            //bBplusTree.InsertRepair(null,null);
        }

        public void Search(TK key)
        {
            bBplusTree.Search(key);
        }

        // Get the proper "Leaf" node(maybe a leaf node in the disk)
        public Node<TK, TV> SearchProperLeafNode(TK key, List<Node<TK, TV>> topToLeafStoreList)
        {
            return bBplusTree.SearchProperLeafNode(key, topToLeafStoreList);
        }

        public void TraverseForword(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            bBplusTree.TraverseForword(node,action);
        }

        public void TraverseBackword(Node<TK, TV> node, Action<Node<TK, TV>> action)
        {
            bBplusTree.TraverseBackword(node, action);
        }

        public Node<TK, TV> SearchInTimes(int times,TK key, List<TV> ridList)
        {
            return bBplusTree.SearchInTimes(times, key, ridList);
        }

        private BPlusTreeProvider(int degree, Node<TK, TV> root)
        {
            bBplusTree = new BPlusTree<TK, TV>(degree);
            this.Root = root;
            this.SearchNode = root;
        }

        public static BPlusTreeProvider<TK, TV> CreatBPlusTree(int degree, Node<TK, TV> root)
        {
            return new BPlusTreeProvider<TK, TV>(degree,root);
        }

        /// <summary>
        /// 修正节点
        /// </summary>
        /// <param name="node"></param>
        /// <param name="isRepairRoot">是否修正根节点</param>
        public void InsertRepair(Node<TK, TV> node, 
            Func<Node<TK, TV>,TV> nodeExportToDisk)
        {
            bBplusTree.InsertRepair(node, nodeExportToDisk);
            Root = bBplusTree.Root;
        }

        public void RepairAfterDelete(Node<TK, TV> node,
            Func<Node<TK, TV>, TV> nodeExportToDisk,Action<Node<TK, TV>> deleteFromDisk)
        {
            bBplusTree.RepairAfterDelete(node, nodeExportToDisk, deleteFromDisk);
            Root = bBplusTree.Root;
        }

        public Node<TK, TV> GetFirstLeafNode()
        {
            Node<TK, TV> firstNode = new Node<TK, TV>();
            bBplusTree.GetFirstLeafNode(Root, ref firstNode);
            return firstNode;
        }
    }
}
