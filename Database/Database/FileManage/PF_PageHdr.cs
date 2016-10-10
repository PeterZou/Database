using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;
using Database.Const;

namespace Database.FileManage
{
    //
    // PF_PageHdr: Header structure for pages
    //
    public struct PF_PageHdr
    {
        public int nextFree;       // nextFree can be any of these values:
                            //  - the number of the next free page
                            //  - PF_PAGE_LIST_END if this is last free page
                            //  - PF_PAGE_USED if the page is not free
    };
}
