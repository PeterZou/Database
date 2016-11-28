using Database.FileManage;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class IX_PageHdr:RM_PageHdr
    {
        // 添加两个字段，一个保存现阶段最大的空闲块，一个保存该page如果经过压缩能够得到的最大块，以决定是不是需要做压缩。
        public int maxBlock;
        public int maxSize;

        public IX_PageHdr(int maxBlock, int maxSize, int numSlots, PF_PageHdr pf_ph) : base(numSlots, pf_ph)
        {
            this.maxBlock = maxBlock;
            this.maxSize = maxSize;
        }

        public IX_PageHdr(int numSlots, PF_PageHdr pf_ph) : base(numSlots, pf_ph)
        {
        }

        public IX_PageHdr() : base()
        {
        }
    }
}
