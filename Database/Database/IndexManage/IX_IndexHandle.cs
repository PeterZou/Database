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

        private Func<string, TK> FuncConverStringToTK;

        public Func<TK, string> FuncConverTKToString;

        private Func<TK> FuncCreatNewTK;

        private int TreeDegree { get; } = 6;

        #region constructor
        public IX_IndexHandle()
        { }

        public IX_IndexHandle(int maxTreeHeightInMemory, NodeDisk<TK> root,
            Func<string, TK> converStringToTK, Func<TK, string> converTKToString, Func<TK> creatNewTK, IX_FileHandle<TK> imp)
        {
            this.imp = imp;

            this.MaxTreeHeightInMemory = maxTreeHeightInMemory;
            this.Root = IndexManagerUtil<TK>.ConvertNodeDiskToNode(root, new RID(-1, -1), FuncCreatNewTK, RootRIDList);

            this.FuncCreatNewTK = creatNewTK;
            this.FuncConverStringToTK = converStringToTK;
            this.FuncConverTKToString = converTKToString;
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

            CreateEntry(key, lastSubRoot);

            int num = 0;
            GetSubTreeUntilTop(lastSubRoot,ref num);
        }

        private void CreateEntry(TK key, Node<TK, RIDKey<TK>> lastSubRoot)
        {
            // add the entry and export to the disk
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, lastSubRoot);

            // TODO Record RID,ought to be imported by record manage, default rid for now
            RID recordRID = default(RID);
            RIDKey<TK> value = new RIDKey<TK>(recordRID, key);
            bPlusTreeProvider.Insert(value);
            
            //Get the leaf child
            var leafNode = bPlusTreeProvider.SearchProperLeafNode(key, null);
            InsertExportToDiskReturn(leafNode);
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

            // do not repair the root
            bPlusTreeProvider.InsertRepair(subRoot,false, InsertExportToDisk);

            if (subRoot.CurrentRID.Rid.CompareTo(Root.CurrentRID.Rid) == 0)
            {
                bPlusTreeProvider.Split(bPlusTreeProvider.Root, true, InsertExportToDisk);
            }
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
                var subRoot = bPlusTreeProvider.SearchProperLeafNode(key, TopToLeafStoreList);

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
                nodeDisk = IndexManagerUtil<TK>.SetCharToNodeDisk(data, data.Length, FuncConverStringToTK);
            }

            // set the node link to the parent
            var node = IndexManagerUtil<TK>.ConvertNodeDiskToNode(nodeDisk, rid, FuncCreatNewTK, RIDList);

            return node;
        }
        #endregion

        #region deltegate to handle this
        // DISK to save
        // TODO which should be saved, async
        public void InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            InsertExportToDiskReturn(node);
        }

        public RID InsertExportToDiskReturn(Node<TK, RIDKey<TK>> node)
        {
            if (node.Values.Count > 1)
            {
                imp.DeleteRec(node.CurrentRID.Rid,node.IsLeaf);
            }

            var nodeDisk = IndexManagerUtil<TK>.ConvertNodeToNodeDisk(node);

            var chars = IndexManagerUtil<TK>.SetNodeDiskToChar(nodeDisk, FuncConverTKToString);

            return imp.InsertRec(chars);
        }
        #endregion

        #region method to use in this class
        
        #endregion
    }
}
