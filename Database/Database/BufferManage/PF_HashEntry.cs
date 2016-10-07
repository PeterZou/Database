using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.BufferManage
{
    public struct PF_HashEntry
    {
        public int fd;
        public int pageNum;
        public int slot;
    }
}
