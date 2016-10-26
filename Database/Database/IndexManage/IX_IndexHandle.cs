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

        public IX_IndexHandle(Iprovider<TK, RIDKey<TK>> ip, IX_IOHandle<TK> iX_IOHandle, int treeHeight)
        {
            providerContext = new ProviderContext<TK, RIDKey<TK>>(ip);
            this.iX_IOHandle = iX_IOHandle;
            this.treeHeight = treeHeight;
        }


        #region Import disk to memory
        // 向上回溯用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDisk(RID rootRid)
        {
            return GetSubTreeFromDisk(rootRid, null);
        }

        // 向下探寻用
        public Node<TK, RIDKey<TK>> GetSubTreeFromDisk(RID rootRid,RIDKey<TK> value)
        {
            // 从root开始，每次导入treeHeight的树
            // 如果发现树中有节点的height达到，以此节点为root
            // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
            // 以root重新构建GetSubTreeFromDisk

            // Get the real Root
            providerContext.SetRoot(iX_IOHandle.InsertImportToMemoryRoot(rootRid));

            Node<TK, RIDKey<TK>> root = null;

            // 设置root
            // 能够一次性完整的放入整棵树
            if (treeHeight >= providerContext.GetRoot().Height)
            {
                root = providerContext.GetRoot();
            }
            else if (treeHeight >= providerContext.GetRoot().Height - treeHeight)
            {
                //如果发现树中有节点的height达到，以此节点为root
            }
            else
            {
                // 如果发现一棵树已经到达子树叶节点，但是仍然没有到达整个树的叶节点，以末节点设为root
            }



            return null;
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

            // insert的回溯处理
            // 1.如果出现分割，需要InsertExportToDisk的Action
            // 2.向上回溯，发现已经到了树的fate Root,重新导入上面的树，直到最后找到真正的Root          
            // 应该是一个递归函数



            providerContext.Insert(value, iX_IOHandle.InsertExportToDisk);
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
