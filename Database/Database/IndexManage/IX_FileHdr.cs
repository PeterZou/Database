using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public struct IX_FileHdr
    {
        public RM_FileHdr rmf;

        public int indexRecordSize;

        // 前几层节点常驻内存，rootpage一次导入,indexRecordSize,degree,pagesize决定，取满B+树
        public int maxRootPageRecordSize;

        // key:value=>indexName:RID
        public char[] indexInfo;
    }
}
