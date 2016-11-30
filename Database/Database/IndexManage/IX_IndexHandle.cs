using Database.Const;
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
        public IX_FileHandle<TK> imp;

        public Node<TK, RIDKey<TK>> SelectNode { set; get; }

        public RID RootRid;

        public IX_IndexHandle(IX_FileHandle<TK> imp, int treeHeight, RID rootRid, 
            Func<string, TK> converStringToTK, Func<TK, string> converTKToString, Func<TK> creatNewTK)
        {
            this.imp = imp;
            this.treeHeight = treeHeight;
            this.RootRid = rootRid;
            bPlusTreeProvider = new BPlusTreeProvider<TK, RIDKey<TK>>(treeHeight);
            this.ConverStringToTK = converStringToTK;
            this.ConverTKToString = converTKToString;
            this.CreatNewTK = creatNewTK;
        }

        public Func<string, TK> ConverStringToTK;

        public Func<TK, string> ConverTKToString;

        public Func<TK> CreatNewTK;

        private BPlusTreeProvider<TK, RIDKey<TK>> bPlusTreeProvider;

        public int treeHeight;

        // 存储从root到底层的相对路径RID,从而实现方向访问
        private List<RIDKey<TK>> ridkeyList = new List<RIDKey<TK>>();

        private Node<TK, RIDKey<TK>> SubRoot { set; get; }
        private Node<TK, RIDKey<TK>> LeafNode { set; get; }
        private List<RID> ridList = new List<RID>();

        #region Import disk to memory
        // 向上回溯用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDisk(RID leafRid)
        {
            // 追溯（保证删除足够完成），从而保证子树的最小化
            // 1.如果高度足够，返回当前root
            // 2.通过leafRid，获取上两层的root,从而重构subtree
            // 递归操作（具体逻辑层展开）：如果上一道两层就能够触及root,找到root,返回root
            int index = ridList.IndexOf(leafRid);
            if (index <= 0) throw new Exception();

            // 1.判断当前子节点前面还有多少parent节点，如果数目少于treeHeight，以ridList[0]为节点构造树
            if (ridList.Count - index + 1 <= treeHeight)
            {
                SubRoot = InsertImportToMemoryRoot(RootRid).Item1;
            }
            else
            {
                //通过leafRid，获取上两层的root,从而重构subtree
                SubRoot = InsertImportToMemoryRoot(ridList[index+treeHeight]).Item1;
            }
            SelectNode = null;
            int tmpHeight = ridList.Count - index + 1 <= treeHeight ? ridList.Count - index + 1 : treeHeight;
            GetSubTreeImportToMemory(tmpHeight, SubRoot.CurrentRID.Rid, null,true, leafRid);
            return SelectNode;
        }

        public void GetSubTreeFromDisk(TK key)
        {
            ridList.Clear();
            ridkeyList.Clear();
            ridkeyList.Add(new RIDKey<TK>(RootRid, key));
            GetSubTreeFromDisk(RootRid, key);
        }

        // 向下探寻用，直到子节点
        private void GetSubTreeFromDisk(RID rootRid,TK key)
        {
            // 从root开始，每次导入treeHeight的树
            // 如果发现树中有节点的height达到，以此节点为root
            // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
            // 以root重新构建GetSubTreeFromDisk

            // Get the real Root
            SubRoot = InsertImportToMemoryRoot(rootRid).Item1;
            GetSubTreeImportToMemory(treeHeight, rootRid, SubRoot, false, default(RID));
            // 获取部分树的root节点
            // 能够一次性完整的放入整棵树
            if (treeHeight >= bPlusTreeProvider.Root.Height || treeHeight >= bPlusTreeProvider.Root.Height - treeHeight)
            {

                if (treeHeight >= bPlusTreeProvider.Root.Height - treeHeight)
                {   //如果发现树中有节点的height达到，以此节点为root
                    // 遍历(providerContext.GetRoot().Height - treeHeight+1)次，找到节点
                    SubRoot = GetRootNode(SubRoot.Height - treeHeight + 1, key);

                    GetSubTreeImportToMemory(treeHeight,
                        bPlusTreeProvider.Root.CurrentRID.Rid, SubRoot,false,default(RID));
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
                SubRoot = GetRootNode(treeHeight, key);
                GetSubTreeFromDisk(SubRoot.CurrentRID.Rid, key);
            }
        }

        private Node<TK, RIDKey<TK>> GetRootNode(int times, TK key)
        {
            var node = bPlusTreeProvider.SearchInTimes(times, key, ridkeyList);
            return node;
        }
        #endregion

        #region Export memory to disk
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
            // 持久化到本地硬盘
            bPlusTreeProvider.Traverse(SubRoot, InsertExportToDisk);

            // 回溯处理
            // 回溯停止条件：1.根节点values>=MaxDegree,2.不是真正的根节点
            while ((SubRoot.Values.Count>= bPlusTreeProvider.bBplusTree.MaxDegree) && 
                (SubRoot.CurrentRID.Rid.CompareTo(RootRid) !=0))
            {
                // 向上重新构建子树，获取新的SubRoot
                GetSubTreeFromDisk(SubRoot.CurrentRID.Rid);
                bPlusTreeProvider.InsertRepair(SubRoot);
                // 持久化到本地硬盘
                bPlusTreeProvider.Traverse(SubRoot, InsertExportToDisk);
            }

        }

        public void DeleteEntry(TK key)
        {
            // 找到叶节点对应的向上最大子树
            GetSubTreeFromDisk(key);

            bPlusTreeProvider.Delete(key);

            // 持久化到本地硬盘
            bPlusTreeProvider.Traverse(SubRoot, DeleteExportToDisk);

            // 回溯处理
            // 回溯停止条件：1.不是真正的根节点,2.根节点values<MinDegree
            while ((SubRoot.Values.Count < bPlusTreeProvider.bBplusTree.MinDegree) &&
                (SubRoot.CurrentRID.Rid.Page != RootRid.Page || SubRoot.CurrentRID.Rid.Slot != RootRid.Slot))
            {
                // 向上重新构建子树，获取新的SubRoot，并将刚才的subRoot对应新的子树叶节点返回给RepairAfterDelete使用
                // 获取当前子树的叶节点
                var node = GetSubTreeFromDisk(SubRoot.CurrentRID.Rid);
                bPlusTreeProvider.RepairAfterDelete(node);
                // 持久化到本地硬盘
                bPlusTreeProvider.Traverse(SubRoot, DeleteExportToDisk);
            }
        }
        #endregion

        public void ForcePages()
        {
            imp.ForcePages(ConstProperty.ALL_PAGES);
        }

        /// <summary>
        /// 在内存中展开一棵子树
        /// </summary>
        /// <param name="height"></param>
        /// <param name="rid">当前结点的RID</param>
        /// <param name="parent">父节点</param>
        public void GetSubTreeImportToMemory(int height, RID rid, Node<TK, RIDKey<TK>> parent, bool useBehind, RID leafRid)
        {
            if (height == 0) return;

            var node = InsertImportToMemory(rid, parent);

            if (useBehind && node.Item1.CurrentRID.Rid.CompareTo(leafRid) == 0)
            {
                SelectNode = node.Item1;
            }

            foreach (var n in node.Item2)
            {
                GetSubTreeImportToMemory(height - 1, n, node.Item1, useBehind, leafRid);
            }
        }

        public Tuple<Node<TK, RIDKey<TK>>, List<RID>> InsertImportToMemoryRoot(RID rid)
        {
            RM_Record record = imp.GetRec(rid);

            char[] data = record.GetData();

            // leaf or not
            NodeDisk<TK> nodeDisk = IndexManagerUtil<TK>.SetCharToNodeDisk(data, data.Length, ConverStringToTK);

            // set the node link to the parent
            var node = ConvertNodeDiskToNode(nodeDisk, rid);

            return node;
        }

        // root rid located in the first page
        // location of the orignal node may not set in right 
        public Tuple<Node<TK, RIDKey<TK>>, List<RID>> InsertImportToMemory(RID rid, Node<TK, RIDKey<TK>> parent)
        {
            var node = InsertImportToMemoryRoot(rid);
            node.Item1.Parent = parent;

            parent.ChildrenNodes.Add(node.Item1);
            return node;
        }

        // 添加单个node到硬盘
        public void InsertExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            var nodeDisk = ConvertNodeToNodeDisk(node);
            char[] data = IndexManagerUtil<TK>.SetNodeDiskToChar(nodeDisk, ConverTKToString);
            // set the nl to the disk
            imp.InsertRec(data);
        }

        // 从硬盘删除单个node
        public void DeleteExportToDisk(Node<TK, RIDKey<TK>> node)
        {
            imp.DeleteRec(node.CurrentRID.Rid);
        }

        private Tuple<Node<TK, RIDKey<TK>>, List<RID>> ConvertNodeDiskToNode(NodeDisk<TK> nodeDisk, RID rid)
        {
            List<RID> ridlist = null;
            Node<TK, RIDKey<TK>> node = new Node<TK, RIDKey<TK>>();
            node.CurrentRID = new RIDKey<TK>(rid, CreatNewTK());
            node.Height = nodeDisk.height;

            // leaf:0,branch:1
            if (nodeDisk.isLeaf == 0)
            {
                node.IsLeaf = true;

                // put the property to the leaf
                for (int i = 0; i < nodeDisk.capacity; i++)
                {
                    node.Property.Add(new RIDKey<TK>(nodeDisk.childRidList[i], nodeDisk.keyList[i]));
                }
            }
            else
            {
                node.IsLeaf = false;
                ridlist = new List<RID>();
                foreach (var v in nodeDisk.childRidList)
                {
                    ridlist.Add(v);
                }
            }
            foreach (var v in nodeDisk.keyList)
                node.Values.Add(v);
            return new Tuple<Node<TK, RIDKey<TK>>, List<RID>>(node, ridlist);
        }

        private NodeDisk<TK> ConvertNodeToNodeDisk(Node<TK, RIDKey<TK>> node)
        {
            NodeDisk<TK> nl = new NodeDisk<TK>();
            // leaf:0,branch:1
            nl.isLeaf = node.IsLeaf == true ? 0 : 1;
            nl.capacity = node.Values.Count;
            nl.keyList = node.Values.ToList();
            nl.height = node.Height;

            foreach (var v in node.Property)
            {
                nl.childRidList.Add(v.Rid);
            }
            nl.length = GetLength(nl);

            return nl;
        }

        private int GetLength(NodeDisk<TK> nl)
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            // TODO Set TK is int
            length += nl.capacity * ConstProperty.Int_Size;
            length += nl.capacity * ConstProperty.RM_Page_RID_SIZE + ConstProperty.RM_Page_RID_SIZE;
            return length;
        }
    }
}
