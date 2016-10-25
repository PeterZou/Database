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
    // TODO:CURSOR
    public class IX_IndexHandle<TK>
        where TK : IComparable<TK>
    {
        private ProviderContext<TK, RIDKey<TK>> providerContext;

        private IX_IOHandle<TK> iX_IOHandle;

        public IX_IndexHandle(Iprovider<TK, RIDKey<TK>> ip, IX_IOHandle<TK> iX_IOHandle)
        {
            providerContext = new ProviderContext<TK, TV>(ip);
            this.iX_IOHandle = iX_IOHandle;
        }


        #region Import disk to memory
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
            // 2.如果root一直在内存，不需要懂，直接开始操作
            // 3.如果root不在内存，需要判断，当前树及其子树是不是能够保证insert
            // 4.如果可以，一直往下循环
            // 5.如果不可以，重新选择一颗新的树

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
