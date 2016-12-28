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

namespace Database.IndexManage
{
    // 一个表中的多个index对应一个文件。
    // 即时写入？一个sql对应的多个List<TV>批量更新B+tree
    // forcepages两种情况，1.index文件关闭，2.切换表，关闭表
    // 内存的索引存储结构，保存父节点，节点，以及下一层节点在内存中
    // 1，搜索：
    // 2，添加：清空缓存，重新设置内存结构，如果发现需要rebalance,递归
    // 3，删除：清空缓存，重新设置内存结构，如果发现需要rebalance,递归
    // ！！！Attention,导入到memory的B+tree是部分的！！！
    // TODO:1.CURSOR 2.缓存
    public class IX_IndexHandle<TK>
        where TK : IComparable<TK>
    {
        #region property
        public IX_FileHandle<TK> imp;

        public Node<TK, RIDKey<TK>> SelectNode { set; get; }

        public bool ChangeOrNot { get; set; }

        public Node<TK, RIDKey<TK>> Root;

        public Func<string, TK> ConverStringToTK;

        public Func<TK, string> ConverTKToString;

        public Func<TK> CreatNewTK;

        private BPlusTreeProvider<TK, RIDKey<TK>> bPlusTreeProvider;

        // 内存中能够展开的最大高度
        public int maxTreeHeightInMemory;

        List<RID> insertRIDList = new List<RID>();

        // 存储从root到底层的相对路径RID,从而实现方向访问
        private List<RIDKey<TK>> ridkeyList = new List<RIDKey<TK>>();

        private Node<TK, RIDKey<TK>> SubRoot { set; get; }
        private List<RID> ridList = new List<RID>();
        #endregion

        #region constructor
        public IX_IndexHandle()
        { }

        public IX_IndexHandle(int maxTreeHeightInMemory, NodeDisk<TK> root, 
            Func<string, TK> converStringToTK, Func<TK, string> converTKToString, Func<TK> creatNewTK, IX_FileHandle<TK> imp)
        {
            // TODO
            int degree = 6;

            this.CreatNewTK = creatNewTK;
            this.maxTreeHeightInMemory = maxTreeHeightInMemory;
            this.Root = IndexManagerUtil<TK>.ConvertNodeDiskToNode(root, new RID(-1,-1), CreatNewTK).Item1;
            bPlusTreeProvider = new BPlusTreeProvider<TK, RIDKey<TK>>(degree, Root);
            this.ConverStringToTK = converStringToTK;
            this.ConverTKToString = converTKToString;
            this.imp = imp;
            ChangeOrNot = false;
        }
        #endregion

        #region creat a subtree from upper and lower
        // 向上回溯用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDiskUpper(RID leafRid)
        {
            // 追溯（保证删除足够完成），从而保证子树的最小化
            // 1.如果高度足够，返回当前root
            // 2.通过leafRid，获取上两层的root,从而重构subtree
            // 递归操作（具体逻辑层展开）：如果上一道两层就能够触及root,找到root,返回root
            int index = ridList.IndexOf(leafRid);
            if (index <= 0) throw new Exception();

            // 1.判断当前子节点前面还有多少parent节点，如果数目少于treeHeight，以ridList[0]为节点构造树
            if (ridList.Count - index + 1 <= maxTreeHeightInMemory)
            {
                SubRoot = IndexManagerUtil<TK>.InsertImportToMemoryRoot(ConstProperty.RootRID
                    ,ConverStringToTK,CreatNewTK,Root,imp).Item1;
            }
            else
            {
                //通过leafRid，获取上两层的root,从而重构subtree
                SubRoot = IndexManagerUtil<TK>.InsertImportToMemoryRoot(ridList[index+maxTreeHeightInMemory]
                    , ConverStringToTK, CreatNewTK, Root, imp).Item1;
            }
            SelectNode = null;
            int tmpHeight = ridList.Count - index + 1 <= maxTreeHeightInMemory ? ridList.Count - index + 1 : maxTreeHeightInMemory;
            IndexManagerUtil<TK>.GetSubTreeImportToMemory(tmpHeight, SubRoot.CurrentRID.Rid, null,true, leafRid
                , ConverStringToTK, CreatNewTK,Root,imp);
            return SelectNode;
        }

        public void GetSubTreeFromDisk(TK key)
        {
            ridList.Clear();
            ridkeyList.Clear();
            GetSubTreeFromDiskLower(ConstProperty.RootRID, key);
        }

        // 向下探寻用，直到子节点
        private void GetSubTreeFromDiskLower(RID rootRid,TK key)
        {
            // 从root开始，每次导入treeHeight的树
            // 如果发现树中有节点的height达到，以此节点为root
            // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
            // 以root重新构建GetSubTreeFromDisk

            // Get the real Root
            SubRoot = IndexManagerUtil<TK>.InsertImportToMemoryRoot(rootRid,ConverStringToTK,CreatNewTK,Root,imp).Item1;
            IndexManagerUtil<TK>.GetSubTreeImportToMemory(maxTreeHeightInMemory, rootRid, SubRoot, false, default(RID)
                , ConverStringToTK, CreatNewTK, Root, imp);
            // 获取部分树的root节点
            // 能够一次性完整的放入整棵树
            if (maxTreeHeightInMemory >= bPlusTreeProvider.Root.Height || maxTreeHeightInMemory >= bPlusTreeProvider.Root.Height - maxTreeHeightInMemory)
            {
                if (maxTreeHeightInMemory >= bPlusTreeProvider.Root.Height)
                { }
                else if (maxTreeHeightInMemory >= bPlusTreeProvider.Root.Height - maxTreeHeightInMemory)
                {   //如果发现树中有节点的height达到，以此节点为root
                    // 遍历(providerContext.GetRoot().Height - treeHeight+1)次，找到节点
                    SubRoot = GetRootNode(SubRoot.Height - maxTreeHeightInMemory + 1, key);

                    IndexManagerUtil<TK>.GetSubTreeImportToMemory(maxTreeHeightInMemory,
                        bPlusTreeProvider.Root.CurrentRID.Rid, SubRoot, false, default(RID)
                        , ConverStringToTK, CreatNewTK, Root, imp);
                }
                bPlusTreeProvider.Root = SubRoot;
                ridkeyList.Add(new RIDKey<TK>(rootRid, key));
                foreach (var r in ridkeyList)
                {
                    ridList.Add(r.Rid);
                }

            }
            else
            {
                // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
                // 遍历到底，找到节点
                SubRoot = GetRootNode(maxTreeHeightInMemory, key);
                GetSubTreeFromDiskLower(SubRoot.CurrentRID.Rid, key);
            }
        }

        private Node<TK, RIDKey<TK>> GetRootNode(int times, TK key)
        {
            var node = bPlusTreeProvider.SearchInTimes(times, key, ridkeyList);
            return node;
        }
        #endregion

        #region operation link to FileHanler
        public RID GetLeafEntry(TK key)
        {
            GetSubTreeFromDisk(key);
            bPlusTreeProvider.Search(key);
            return bPlusTreeProvider.SearchNode.CurrentRID.Rid;
        }

        public void InsertEntry(RIDKey<TK> value)
        {
            // combing through it
            // 1.首先确定能够容纳几层的node在memory
            // 2.确定能抵达叶节点的最低子树
            GetSubTreeFromDisk(value.Key);

            bPlusTreeProvider.Insert(value);

            // 12152016
            SubRoot = bPlusTreeProvider.bBplusTree.Root;

            // 持久化到本地硬盘
            bPlusTreeProvider.TraverseBackword(SubRoot, InsertExportToDisk);

            // 回溯处理
            // 回溯停止条件：1.根节点values>=MaxDegree,2.不是真正的根节点
            while ((SubRoot.Values.Count>= bPlusTreeProvider.bBplusTree.MaxDegree) && 
                (SubRoot.CurrentRID.Rid.CompareTo(ConstProperty.RootRID) !=0))
            {
                // 向上重新构建子树，获取新的SubRoot
                GetSubTreeFromDiskUpper(SubRoot.CurrentRID.Rid);
                bPlusTreeProvider.InsertRepair(SubRoot);
                // 持久化到本地硬盘
                bPlusTreeProvider.TraverseBackword(SubRoot, InsertExportToDisk);
            }

        }

        public void DeleteEntry(TK key)
        {
            // 找到叶节点对应的向上最大子树
            GetSubTreeFromDisk(key);

            bPlusTreeProvider.Delete(key);

            // 持久化到本地硬盘
            bPlusTreeProvider.TraverseForword(SubRoot, DeleteExportToDisk);

            // 回溯处理
            // 回溯停止条件：1.不是真正的根节点,2.根节点values<MinDegree
            while ((SubRoot.Values.Count < bPlusTreeProvider.bBplusTree.MinDegree) &&
                (SubRoot.CurrentRID.Rid.CompareTo(ConstProperty.RootRID) != 0))
            {
                // 向上重新构建子树，获取新的SubRoot，并将刚才的subRoot对应新的子树叶节点返回给RepairAfterDelete使用
                // 获取当前子树的叶节点
                var node = GetSubTreeFromDiskUpper(SubRoot.CurrentRID.Rid);
                bPlusTreeProvider.RepairAfterDelete(node);
                // 持久化到本地硬盘
                bPlusTreeProvider.TraverseForword(SubRoot, DeleteExportToDisk);
            }
        }

        public void ForcePages()
        {
            imp.ForcePages(ConstProperty.ALL_PAGES);
        }

        // 添加单个node到硬盘
        public void InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            var nodeDisk = IndexManagerUtil<TK>.ConvertNodeToNodeDisk(node);

            // Root node
            if (node.Parent == null)
            {
                if (node.IsLeaf == false)
                {
                    nodeDisk.childRidList.Clear();
                    for (int i=0;i<insertRIDList.Count;i++)
                    {
                        node.ChildrenNodes[i].CurrentRID = new RIDKey<TK>(insertRIDList[i], default(TK));
                        nodeDisk.childRidList.Add(insertRIDList[i]);
                    }

                    insertRIDList.Clear();
                }

                Root = node;
                imp.hdr.root = nodeDisk;
                ChangeOrNot = true;
            }
            else
            {
                // Non-leaf
                if (node.IsLeaf == false)
                {
                    foreach (var i in insertRIDList)
                    {
                        nodeDisk.childRidList.Add(i);
                    }

                    insertRIDList.Clear();
                }
                
                // set the nl to the disk
                char[] data = IndexManagerUtil<TK>.SetNodeDiskToChar(nodeDisk, ConverTKToString);
                var currentRID = imp.InsertRec(data);

                if (node.IsLeaf == true)
                {
                    insertRIDList.Add(currentRID);
                }
            }
        }

        // 从硬盘删除单个node
        public void DeleteExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            imp.DeleteRec(node.CurrentRID.Rid);
        }
        #endregion
    }
}
