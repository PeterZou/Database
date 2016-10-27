using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.RecordManage
{
    public struct RM_FileHdr
    {
        public PF_FileHdr pf_fh;
        public int extRecordSize;
        public int dataNum;
        public char[] data;
    }
}
