using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.FileManage
{
    public struct PF_FileHdr
    {
        public int firstFree;     // first free page in the linked list
        public int numPages;      // # of pages in the file
    }
}
