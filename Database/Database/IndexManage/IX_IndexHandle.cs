using Database.IndexManage.BPlusTree;
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
    public class IX_IndexHandle<TK, TV>
        where TV : INode<TK>
        where TK : IComparable<TK>
    {
        private ProviderContext<TK, TV> providerContext;

        

        public IX_IndexHandle(Iprovider<TK, TV> ip)
        {
            providerContext = new ProviderContext<TK, TV>(ip);
        }

        public void InsertEntry(List<TV> values)
        {
            foreach (var v in values)
            {
                InsertEntry(v);
            }
        }

        public void DeleteEntry(List<TK> key)
        {
            foreach (var k in key)
            {
                providerContext.Delete(k);
            }
        }

        public void ForcePages()
        {

        }

        private void InsertEntry(TV value)
        {
            // 清空内存中的B+tree

            // 重新设置内存B+tree

            //内存中
            providerContext.Insert(value);


        }
    }
}
