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
        private ProviderContext<TK, RIDKey<TK>> providerContext;

        private IX_IOHandle<TK> iX_IOHandle;

        private int treeHeight;

        public RID RootRid;

        // 存储从root到底层的相对路径RID,从而实现方向访问
        private List<RIDKey<TK>> ridList = new List<RIDKey<TK>>();

        private Node<TK, RIDKey<TK>> SubRoot { set; get; }

        public IX_IndexHandle(Iprovider<TK, RIDKey<TK>> ip, IX_IOHandle<TK> iX_IOHandle, int treeHeight, RID rootRid)
        {
            providerContext = new ProviderContext<TK, RIDKey<TK>>(ip);
            this.iX_IOHandle = iX_IOHandle;
            this.treeHeight = treeHeight;
            this.RootRid = rootRid;
        }


        #region Import disk to memory
        // 向上回溯用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDisk(RID leafRid)
        {
            // 两层两层的追溯（保证删除足够完成），从而保证子树的最小化
            // 通过leafRid，获取上两层的root,从而重构subtree
            // 1.如果高度足够，返回当前root
            // 2.如果上一道两层就能够触及root,找到root,返回root
            return null;
        }

        public void GetSubTreeFromDisk(RIDKey<TK> value)
        {
            ridList.Clear();
            ridList.Add(new RIDKey<TK>(RootRid, value.Key));
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
            if (treeHeight >= providerContext.GetRoot().Height)
            {
                providerContext.SetRoot(SubRoot);
                ridList.Add(new RIDKey<TK>(rootRid, value.Key));
            }
            else if (treeHeight >= providerContext.GetRoot().Height - treeHeight)
            {
                //如果发现树中有节点的height达到，以此节点为root
                // 遍历(providerContext.GetRoot().Height - treeHeight+1)次，找到节点
                SubRoot = GetRootNode(SubRoot.Height - treeHeight + 1, value);
                providerContext.SetRoot(SubRoot);
                iX_IOHandle.GetSubTreeImportToMemory(treeHeight, 
                    providerContext.GetRoot().CurrentRID.Rid, SubRoot);
                ridList.Add(new RIDKey<TK>(rootRid, value.Key));
            }
            else
            {
                // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
                // 遍历到底，找到节点
                SubRoot = GetRootNode(treeHeight,value);
                GetSubTreeFromDisk(SubRoot.CurrentRID.Rid, value);
            }
        }

        private Node<TK, RIDKey<TK>> GetRootNode(int times, RIDKey<TK> value)
        {
            // TODO
            // ridStack.Push(node.CurrentRID.Rid);

            var node = providerContext.SearchInTimes(times, value, ridList);
            return node;
        }
        #endregion

        #region Export memory to disk
        public void ForcePages()
        {
            iX_IOHandle.ForcePages();
        }

        public void InsertEntry(RIDKey<TK> value)
        {
            // get the deepest tree and insert
            // add a action<> into insert in order to create a record to make the new insert node rid

            // combing through it
            // 1.首先确定能够容纳几层的node在memory
            // 2.确定能抵达叶节点的最低子树
            GetSubTreeFromDisk(value);

            // insert的回溯处理
            // 1.如果出现分割，需要InsertExportToDisk的Action
            // 2.向上回溯，发现已经到了树的fate Root,重新导入上面的树，直到最后找到真正的Root          
            // 应该是一个递归函数
            // providerContext.Insert(value, iX_IOHandle.InsertExportToDisk);

        }

        public void DeleteEntry(TK key)
        {
            // combing through it
            // the same as above

            // delete的回溯处理
            // the same as above
        }
        #endregion
    }
}
