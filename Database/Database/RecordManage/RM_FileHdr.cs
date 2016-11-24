using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.RecordManage
{
    public class RM_FileHdr: PF_FileHdr
    {
        public int extRecordSize;

        public char[] data;
    }
}
