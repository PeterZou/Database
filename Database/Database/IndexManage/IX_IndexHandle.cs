using Database.Const;
using Database.IndexManage;
using Database.IndexManage.BPlusTree;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database
{
    public class IX_IndexHandle<TK>
        where TK : IComparable<TK>
    {
        public IX_FileHandle<TK> imp;

        private Node<TK, RIDKey<TK>> Root;

        private List<RID> RootRIDList = new List<RID>();

        private List<Node<TK, RIDKey<TK>>> TopToLeafStoreList { get; set; } 
            = new List<Node<TK, RIDKey<TK>>>();
        
        // 内存中能够展开的最大高度
        private int MaxTreeHeightInMemory { get; }

        private Func<string, TK> ConverStringToTK;

        public Func<TK, string> ConverTKToString;

        private Func<TK> CreatNewTK;

        private int TreeDegree { get; } = 6;

        #region constructor
        public IX_IndexHandle()
        { }

        public IX_IndexHandle(int maxTreeHeightInMemory, NodeDisk<TK> root,
            Func<string, TK> converStringToTK, Func<TK, string> converTKToString, Func<TK> creatNewTK, IX_FileHandle<TK> imp)
        {
            this.imp = imp;

            this.MaxTreeHeightInMemory = maxTreeHeightInMemory;
            this.Root = IndexManagerUtil<TK>.ConvertNodeDiskToNode(root, new RID(-1, -1), CreatNewTK, RootRIDList);

            this.CreatNewTK = creatNewTK;
            this.ConverStringToTK = converStringToTK;
            this.ConverTKToString = converTKToString;
        }
        #endregion

        #region Link to IX_FileHandle
        public RID GetLeafEntry(TK key)
        {
            throw new NotImplementedException();
        }

        public void InsertEntry(TK key)
        {
            var lastSubRoot = GetSubTreeUntilLeaf(key);

            int num = 0;
            GetSubTreeUntilTop(lastSubRoot,ref num);
        }
        #endregion

        #region operation
        private Node<TK, RIDKey<TK>> GetSubTreeUntilLeaf(TK key)
        {
            TopToLeafStoreList.Clear();
            Node<TK, RIDKey<TK>> lastSubRoot = null;
            GetSubTreeUntilLeaf(key, Root, RootRIDList, ref lastSubRoot); 
            return lastSubRoot;
        }

        private void GetSubTreeUntilTop(Node<TK, RIDKey<TK>> subRootNode,ref int index)
        {
            int TotolHeight = Root.Height;
            // Two nums must match
            if (TopToLeafStoreList.Count != TotolHeight) throw new Exception();
            // construct a tree
            if (TotolHeight <= MaxTreeHeightInMemory)
            {
                InsertRepair(Root);
            }
            else
            {
                index += MaxTreeHeightInMemory;
                var headTree = TopToLeafStoreList[index];
                InsertRepair(headTree);

                GetSubTreeUntilTop(headTree, ref index);
            }
        }

        private void InsertRepair(Node<TK, RIDKey<TK>> subRoot)
        {
            // import all of it
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, subRoot);
            bPlusTreeProvider.InsertRepair(subRoot);

            // DISK to save
            // TODO which should be saved, async
            // TODO or use delegate?
        }

        private void GetSubTreeUntilLeaf(TK key, Node<TK, RIDKey<TK>> node, List<RID> RIDList,
            ref Node<TK, RIDKey<TK>> refSubRoot)
        {
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, node);
            if (node.Height <= MaxTreeHeightInMemory)
            {
                // import all the tree
                refSubRoot = ImportToBPlusTreeProvider(node, RIDList, bPlusTreeProvider);

            }
            else
            {
                // import all the tree and search the leaf
                var treeHead = ImportToBPlusTreeProvider(node, RIDList, bPlusTreeProvider);
                var subRoot = bPlusTreeProvider.SearchProperNode(key, TopToLeafStoreList);

                // Recusive
                // Head
                GetSubTreeUntilLeaf(key, subRoot, RIDList, ref refSubRoot);
            }
        }

        private Node<TK, RIDKey<TK>> ImportToBPlusTreeProvider(Node<TK, RIDKey<TK>> node, 
            List<RID> RIDList, BPlusTreeProvider<TK, RIDKey<TK>> bPlusTreeProvider)
        {
            // times<=MaxTreeHeightInMemory
            int maxTimes = node.Height < MaxTreeHeightInMemory ? node.Height : MaxTreeHeightInMemory;

            ImportByTimes(node, RIDList, maxTimes);

            return node;
        }

        private void ImportByTimes(Node<TK, RIDKey<TK>> node, List<RID> RIDList, int times)
        {
            if (times != 0 && node.IsLeaf == false)
            {
                foreach (var r in RIDList)
                {
                    List<RID> subRIDList = new List<RID>();
                    var childNode = ImportOneNode(r, subRIDList);
                    node.ChildrenNodes.Add(childNode);
                    childNode.Parent = node;
                    ImportByTimes(childNode, subRIDList, times - 1);
                }
            }
        }

        public Node<TK, RIDKey<TK>> ImportOneNode(RID rid, List<RID> RIDList)
        {
            NodeDisk<TK> nodeDisk;
            if (rid.CompareTo(ConstProperty.RootRID) == 0)
            {
                nodeDisk = IndexManagerUtil<TK>.ConvertNodeToNodeDisk(Root);
            }
            else
            {
                RM_Record record = imp.GetRec(rid);

                char[] data = record.GetData();

                // leaf or not
                nodeDisk = IndexManagerUtil<TK>.SetCharToNodeDisk(data, data.Length, ConverStringToTK);
            }

            // set the node link to the parent
            var node = IndexManagerUtil<TK>.ConvertNodeDiskToNode(nodeDisk, rid, CreatNewTK, RIDList);

            return node;
        }
        #endregion

        #region deltegate to handle this
        public void InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region method to use in this class
        #endregion
    }
}
