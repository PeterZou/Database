using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class IX_PageHdr
    {
        public RM_PageHdr hdr;
        // 添加两个字段，一个保存现阶段最大的空闲块，一个保存该page如果经过压缩能够得到的最大块，以决定是不是需要做压缩。
        public int maxBlock;
        public int maxSize;
    }
}
