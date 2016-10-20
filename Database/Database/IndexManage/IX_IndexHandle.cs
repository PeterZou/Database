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
    public class IX_IndexHandle<TK, TV>
    {
        private ProviderContext<TK, TV> providerContext;

        private RM_FileHandle rmp;

        public IX_IndexHandle(Iprovider<TK, TV> ip)
        {
            providerContext = new ProviderContext<TK, TV>(ip);
        }

        public void InsertEntry(List<TV> value)
        {
            foreach (var v in value)
            {
                providerContext.Insert(v);
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
    }
}
