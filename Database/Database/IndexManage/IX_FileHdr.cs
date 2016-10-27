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
    public struct IX_FileHdr
    {
        public PF_FileHdr pf_fh;
        public int extRecordSize;

        // RM_FileHdr.data 作为结点常驻内存
        public int height;

        public ConstProperty.AttrType indexType;
    }
}
