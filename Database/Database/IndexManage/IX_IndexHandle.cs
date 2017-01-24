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

        public Node<TK, RIDKey<TK>> Root;

        private List<RID> RootRIDList = new List<RID>();

        private List<Node<TK, RIDKey<TK>>> TopToLeafStoreList { get; set; }
            = new List<Node<TK, RIDKey<TK>>>();

        // 内存中能够展开的最大高度
        private int MaxTreeHeightInMemory { get; }

        private Func<string, TK> FuncConverStringToTK;

        public Func<TK, string> FuncConverTKToString;

        private Func<TK> FuncCreatNewTK;

        private int TreeDegree { get; }

        #region constructor
        public IX_IndexHandle()
        { }

        public IX_IndexHandle(int maxTreeHeightInMemory, RID rootRID,
            Func<string, TK> converStringToTK, Func<TK, string> converTKToString,
            Func<TK> creatNewTK, IX_FileHandle<TK> imp,int treeDegree)
        {
            this.imp = imp;

            this.MaxTreeHeightInMemory = maxTreeHeightInMemory;

            this.FuncCreatNewTK = creatNewTK;
            this.FuncConverStringToTK = converStringToTK;
            this.FuncConverTKToString = converTKToString;
            this.TreeDegree = treeDegree;
            GetRootEntry(rootRID);
        }
        #endregion

        #region Link to IX_FileHandle
        public void GetRootEntry(RID rid)
        {
            var rec = imp.GetRec(rid);
            RootRIDList.Clear();

            if (rec != null)
            {
                var nodeDisk = IndexManagerUtil<TK>.SetCharToNodeDisk(rec.data, rec.data.Length,
                    FuncConverStringToTK);
                Root = IndexManagerUtil<TK>.ConvertNodeDiskToNode(nodeDisk, rid,
                    FuncCreatNewTK, RootRIDList);
            }
            else
            {
                Root = new Node<TK, RIDKey<TK>>();
                Root.CurrentRID = new RIDKey<TK>(new RID(-1, -1), default(TK));
            }

        }

        public void InsertEntry(RIDKey<TK> key)
        {
            GetRootEntry(Root.CurrentRID.Rid);
            var lastSubRoot = GetSubTreeUntilLeaf(key.Key);

            CreateEntry(key, lastSubRoot);

            int num = 0;
            GetSubTreeUntilTop(lastSubRoot, ref num, key.Key, InsertRepair);

            while (Root.Parent != null)
            {
                Root = Root.Parent;
            }
        }

        public void DeleteEntry(TK key)
        {
            GetRootEntry(Root.CurrentRID.Rid);
            var lastSubRoot = GetSubTreeUntilLeaf(key);

            RemoveEntry(key, lastSubRoot);

            int num = 0;
            GetSubTreeUntilTop(lastSubRoot, ref num, key, RepairAfterDelete);

            while (Root.Parent != null)
            {
                Root = Root.Parent;
            }
        }

        private void CreateEntry(RIDKey<TK> key, Node<TK, RIDKey<TK>> lastSubRoot)
        {
            // add the entry and export to the disk
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, lastSubRoot);

            // TODO Record RID,ought to be imported by record manage, default rid for now

            //Get the leaf child
            var leafNode = bPlusTreeProvider.SearchProperLeafNode(key.Key, null);
            if (leafNode.Values.Count != TreeDegree - 1)
            {
                bPlusTreeProvider.Insert(key);

                NodeExportToDisk(leafNode);
            }
            else
            {
                leafNode.Property.Add(key);
                leafNode.Values.Add(key.Key);
            }
        }

        private void RemoveEntry(TK key, Node<TK, RIDKey<TK>> lastSubRoot)
        {
            // add the entry and export to the disk
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, lastSubRoot);

            //Get the leaf child
            var leafNode = bPlusTreeProvider.SearchProperLeafNode(key, null);
            if (leafNode.Values.Count != TreeDegree / 2)
            {
                bPlusTreeProvider.Delete(key);

                NodeExportToDisk(leafNode);
            }
            else
            {
                int index = leafNode.Values.IndexOf(key);
                leafNode.Values.Remove(key);
                leafNode.Property.RemoveAt(index);
            }
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

        private Node<TK, RIDKey<TK>> GetSubTreeUntilSpecialNode(bool isSmallest)
        {
            TopToLeafStoreList.Clear();
            Node<TK, RIDKey<TK>> lastSubRoot = null;
            GetSubTreeUntilSpecialNode(Root, RootRIDList, ref lastSubRoot, isSmallest);
            return lastSubRoot;
        }

        private void GetSubTreeUntilTop(Node<TK, RIDKey<TK>> subRootNode, ref int index, TK key,
            Action<Node<TK, RIDKey<TK>>, TK> action)
        {
            int TotolHeight = Root.Height;
            // Two nums must match
            //if (TopToLeafStoreList.Count != TotolHeight) throw new Exception();
            // construct a tree
            if (TotolHeight <= MaxTreeHeightInMemory)
            {
                action(Root,key);
            }
            else
            {
                index += MaxTreeHeightInMemory;
                var headTree = TopToLeafStoreList[index];
                action(headTree,key);

                GetSubTreeUntilTop(headTree, ref index,key, action);
            }
        }

        private void InsertRepair(Node<TK, RIDKey<TK>> subRoot, TK key)
        {
            // import all of it
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, subRoot);

            var node = bPlusTreeProvider.SearchProperLeafNode(key,null);
            // do not repair the root
            bPlusTreeProvider.InsertRepair(node, NodeExportToDisk);

            Root = bPlusTreeProvider.Root;
        }

        private void RepairAfterDelete(Node<TK, RIDKey<TK>> subRoot, TK key)
        {
            // import all of it
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, subRoot);

            var node = bPlusTreeProvider.SearchProperLeafNode(key, null);
            // do not repair the root
            bPlusTreeProvider.RepairAfterDelete(node, NodeExportToDisk, DeleteFromDisk);

            Root = bPlusTreeProvider.Root;
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

        private void GetSubTreeUntilSpecialNode(Node<TK, RIDKey<TK>> node, List<RID> RIDList,
           ref Node<TK, RIDKey<TK>> refSubRoot,bool isSamllest)
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
                var subRoot = new Node<TK, RIDKey<TK>>();
                if (isSamllest)
                { subRoot = bPlusTreeProvider.GetFirstLeafNode(); }
                else
                { subRoot = bPlusTreeProvider.GetLastLeafNode(); }
                
                // Recusive
                // Head
                GetSubTreeUntilSpecialNode(subRoot, RIDList, ref refSubRoot, isSamllest);
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
                node.ChildrenNodes = new List<Node<TK, RIDKey<TK>>>();
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

        public Node<TK, RIDKey<TK>> FindSmallestLeaf()
        {
            GetRootEntry(Root.CurrentRID.Rid);

            var lastSubRoot = GetSubTreeUntilSpecialNode(true);
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, lastSubRoot);
            return bPlusTreeProvider.GetFirstLeafNode();
        }

        public Node<TK, RIDKey<TK>> FindLargestLeaf()
        {
            GetRootEntry(Root.CurrentRID.Rid);

            var lastSubRoot = GetSubTreeUntilSpecialNode(false);
            var bPlusTreeProvider = BPlusTreeProvider<TK, RIDKey<TK>>.CreatBPlusTree(TreeDegree, lastSubRoot);
            return bPlusTreeProvider.GetLastLeafNode();
        }

        public Node<TK, RIDKey<TK>> FindNextLeafNode(Node<TK, RIDKey<TK>> leafNode)
        {
            var leafNodeRID = leafNode.NextNode.Rid;

            //Just import leaf node
            return ImportOneNode(leafNodeRID, null);
        }

        public Node<TK, RIDKey<TK>> FindPreviousLeafNode(Node<TK, RIDKey<TK>> leafNode)
        {
            var leafNodeRID = leafNode.PreviousNode.Rid;

            //Just import leaf node
            return ImportOneNode(leafNodeRID, null);
        }
        #endregion

        #region deltegate to handle this
        // DISK to save
        // TODO which should be saved, async
        public RIDKey<TK> NodeExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            if (node.Values != null)
            {
                DeleteFromDisk(node);

                if (node.Values.Count != 0)
                {
                    var nodeDisk = IndexManagerUtil<TK>.ConvertNodeToNodeDisk(node);

                    var chars = IndexManagerUtil<TK>.SetNodeDiskToChar(nodeDisk, FuncConverTKToString);

                    RID rid = imp.InsertRec(chars);

                    // 向上递归
                    if (node.CurrentRID == null)
                    {
                        node.CurrentRID = new RIDKey<TK>(rid, default(TK));
                    }
                    else
                    {
                        node.CurrentRID.Rid = rid;
                    }

                    if (node.Parent != null && node.Parent.ChildrenNodes != null)
                    {
                        bool flag = true;
                        foreach (var p in node.Parent.ChildrenNodes)
                        {
                            if (p.CurrentRID == null)
                            {
                                flag = false;
                                break;
                            }
                        }
                        if (flag == true)
                        {
                            ResetNodeToParentLink(node);
                        }

                    }

                    // 如果root节点发生变化，重置root节点
                    // TODO
                    if (node.CurrentRID != null)
                    {
                        Root = node;
                        Root.CurrentRID.Rid = rid;
                        if (node.ChildrenNodes != null)
                        {
                            RootRIDList = node.ChildrenNodes.Select(p => p.CurrentRID.Rid).ToList();
                        }
                    }

                    if (node.IsLeaf && node.NextNode!= null && node.PreviousNode != null)
                    {
                        ResetSlibings(node, true);
                        ResetSlibings(node, false);
                    }
                    
                    return new RIDKey<TK>(rid, default(TK));
                }
            }

            return new RIDKey<TK>(new RID(-1,-1), default(TK));
        }

        private void ResetSlibings(Node<TK, RIDKey<TK>> node, bool isLeft)
        {
            RID tmpRID; 
            if (isLeft)
                tmpRID = node.PreviousNode.Rid;
            else
                tmpRID = node.NextNode.Rid;
            if (tmpRID.Page != -1 && tmpRID.Slot != -1)
            {
                var rec = imp.GetRec(tmpRID);
                var nodeInDisk = IndexManagerUtil<TK>.SetCharToNodeDisk(rec.data,
                    rec.data.Length,
                    FuncConverStringToTK);

                if (isLeft)
                    nodeInDisk.rightRID = node.CurrentRID.Rid;
                else
                    nodeInDisk.leftRID = node.CurrentRID.Rid;
                // Update
                rec.data = IndexManagerUtil<TK>.SetNodeDiskToChar(nodeInDisk,
                    imp.ConverTKToString);

                imp.UpdateRec(rec);
            }
        }

        public void DeleteFromDisk(Node<TK, RIDKey<TK>> node)
        {
            if (node.CurrentRID != null)
            {
                imp.DeleteRec(node.CurrentRID.Rid, node.IsLeaf);
            }
        }

        private void ResetNodeToParentLink(Node<TK, RIDKey<TK>> node)
        {
            if (node.Parent != null)
            {
                NodeExportToDisk(node.Parent);

                ResetNodeToParentLink(node.Parent);
            }
        }
        #endregion
    }
}
