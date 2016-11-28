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
        public int totalHeight;

        public ConstProperty.AttrType indexType;

        // Root
        NodeDisk<TK> root;

        // K:V=>Degree:fistFreePage

    }
}
