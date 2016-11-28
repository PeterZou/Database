using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.FileManage;
using Database.IndexManage.IndexValue;

namespace Database.IndexManage
{
    public class IX_FileHdr<TK> : PF_FileHdr
    {
        // int
        public int extRecordSize;

        // int
        public int totalHeight;

        // 只占一个字节
        public ConstProperty.AttrType indexType;

        // Root
        public NodeDisk<TK> root;

        // K:V=>Degree:fistFreePage 
        // K=>ConstProperty.IndexHeaderKey,
        // V=>ConstProperty.IndexHeaderValue
        // total=>ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue
        public int dicCount;
        public Dictionary<int, int> dic;

        public IX_FileHdr()
        {

        }
    }
}
