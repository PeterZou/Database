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

        // key:value=>indexName:pagenum
        public char[] indexInfo;
    }
}
