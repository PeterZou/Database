using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Database.Const;

namespace Database.FileManage
{
    public struct PF_PageHdr
    {
        //Integer
        public int pageNum;

        public int nextFree;
    }
}
