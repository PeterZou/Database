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
        private BPlusTreeProvider<TK, RIDKey<TK>> bPlusTreeProvider;

        private IX_IOHandle<TK> iX_IOHandle;

        private int treeHeight;

        public RID RootRid;

        // 存储从root到底层的相对路径RID,从而实现方向访问
        private List<RIDKey<TK>> ridkeyList = new List<RIDKey<TK>>();

        private Node<TK, RIDKey<TK>> SubRoot { set; get; }
        private List<RID> ridList = new List<RID>();

        public IX_IndexHandle(BPlusTreeProvider<TK, RIDKey<TK>> ip, IX_IOHandle<TK> iX_IOHandle, int treeHeight, RID rootRid)
        {
            bPlusTreeProvider = ip;
            this.iX_IOHandle = iX_IOHandle;
            this.treeHeight = treeHeight;
            this.RootRid = rootRid;
        }


        #region Import disk to memory
        // 向上回溯用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDisk(RID leafRid)
        {
            // 两层两层的追溯（保证删除足够完成），从而保证子树的最小化
            // 2.通过leafRid，获取上两层的root,从而重构subtree
            // 1.如果高度足够，返回当前root
            // 递归操作（具体逻辑层展开）：如果上一道两层就能够触及root,找到root,返回root
            int index = ridList.IndexOf(leafRid);
            if (index <= 0) throw new Exception();

            // 1.判断当前子节点前面还有多少parent节点，如果数目少于treeHeight，以ridList[0]为节点构造树
            if (ridList.Count - index + 1 <= treeHeight)
            {
                return iX_IOHandle.InsertImportToMemoryRoot(RootRid).Item1;
            }
            else
            {
                //通过leafRid，获取上两层的root,从而重构subtree
                return iX_IOHandle.InsertImportToMemoryRoot(ridList[index+treeHeight]).Item1;
            }

            throw new Exception();
        }

        public void GetSubTreeFromDisk(RIDKey<TK> value)
        {
            ridList.Clear();
            ridkeyList.Clear();
            ridkeyList.Add(new RIDKey<TK>(RootRid, value.Key));
            GetSubTreeFromDisk(RootRid, value);
        }

        // 向下探寻用，直到子节点
        private void GetSubTreeFromDisk(RID rootRid,RIDKey<TK> value)
        {
            // 从root开始，每次导入treeHeight的树
            // 如果发现树中有节点的height达到，以此节点为root
            // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
            // 以root重新构建GetSubTreeFromDisk

            // Get the real Root
            SubRoot = iX_IOHandle.InsertImportToMemoryRoot(rootRid).Item1;
            iX_IOHandle.GetSubTreeImportToMemory(treeHeight, rootRid, SubRoot);
            // 获取部分树的root节点
            // 能够一次性完整的放入整棵树
            if (treeHeight >= bPlusTreeProvider.Root.Height || treeHeight >= bPlusTreeProvider.Root.Height - treeHeight)
            {

                if (treeHeight >= bPlusTreeProvider.Root.Height - treeHeight)
                {   //如果发现树中有节点的height达到，以此节点为root
                    // 遍历(providerContext.GetRoot().Height - treeHeight+1)次，找到节点
                    SubRoot = GetRootNode(SubRoot.Height - treeHeight + 1, value);

                    iX_IOHandle.GetSubTreeImportToMemory(treeHeight,
                        bPlusTreeProvider.Root.CurrentRID.Rid, SubRoot);
                }
                bPlusTreeProvider.Root = SubRoot;
                ridkeyList.Add(new RIDKey<TK>(rootRid, value.Key));
                foreach (var r in ridkeyList)
                {
                    ridList.Add(r.Rid);
                }

            }
            else
            {
                // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
                // 遍历到底，找到节点
                SubRoot = GetRootNode(treeHeight, value);
                GetSubTreeFromDisk(SubRoot.CurrentRID.Rid, value);
            }
        }

        private Node<TK, RIDKey<TK>> GetRootNode(int times, RIDKey<TK> value)
        {
            // TODO
            // ridStack.Push(node.CurrentRID.Rid);

            var node = bPlusTreeProvider.SearchInTimes(times, value, ridkeyList);
            return node;
        }
        #endregion

        #region Export memory to disk
        public void ForcePages()
        {
            iX_IOHandle.ForcePages();
        }

        //TODO
        public RID GetEntry(TK key)
        {
            return default(RID);
        }

        public void InsertEntry(RIDKey<TK> value)
        {
            // combing through it
            // 1.首先确定能够容纳几层的node在memory
            // 2.确定能抵达叶节点的最低子树
            GetSubTreeFromDisk(value);

            bPlusTreeProvider.Insert(value);
            // 持久化到本地硬盘
            iX_IOHandle.InsertExportToDisk(SubRoot);

            // 回溯处理
            // 回溯停止条件：1.根节点values>=Degree,2.不是真正的根节点
            while ((SubRoot.Values.Count>= bPlusTreeProvider.bBplusTree.Degree) && 
                (SubRoot.CurrentRID.Rid.Page != RootRid.Page || SubRoot.CurrentRID.Rid.Slot != RootRid.Slot))
            {
                // 向上重新构建子树，获取新的SubRoot
                SubRoot = GetSubTreeFromDisk(SubRoot.CurrentRID.Rid);
                bPlusTreeProvider.InsertRepair(SubRoot);
                // 持久化到本地硬盘
                iX_IOHandle.InsertExportToDisk(SubRoot);
            }

        }

        public void DeleteEntry(TK key)
        {
            // 找到叶节点对应的向上最大子树
            //GetSubTreeFromDisk();

            bPlusTreeProvider.Delete(key);

            // 持久化到本地硬盘
            //iX_IOHandle.DeleteExportToDisk();

            // 回溯处理
            // TODO
        }
        #endregion
    }
}
