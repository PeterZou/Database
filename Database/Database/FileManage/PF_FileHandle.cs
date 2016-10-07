using Database.BufferManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static Database.Const.ConstProperty;

/// <summary>
/// TODO:1.why need to PF_FileHdr?
/// </summary>
namespace Database.FileManage
{
    /// <summary>
    /// Convert the file in the disk to the buffer in the memory
    /// So the file is the saved as stream and we need some method to put into the buffer
    /// Unlike the C++ to control the pointer, we must define the struct or class to keep the type is right
    /// </summary>
    public class PF_FileHandle
    {
        private PF_Buffermgr pf_bm;
        private bool bFileOpen;                                 // file open flag
        private int bHdrChanged;                               // dirty flag for file hdr
        private int fd;                                        // OS file descriptor
        private PF_PageHandle pf_ph;

        public PF_FileHandle()
        {
            bFileOpen = false;
        }

        //
        // GetThisPage
        //
        // Desc: Get a specific page in a file
        //       The file handle must refer to an open file
        // In:   pageNum - the number of the page to get
        // Out:  pageHandle - becomes a handle to the this page of the file
        //                    this function modifies local var's in pageHandle
        //       The referenced page is pinned in the buffer pool.
        // Ret:  PF return code
        //
        public void GetThisPage(int pageNum)
        {
            if (IsValidPageNum(pageNum)) throw new Exception();

            if (pf_bm == null) throw new Exception();

            string pageContent = pf_bm.GetPage(fd, pageNum, true);

            // int is occupided 1B
            pf_ph.PageNum = Convert.ToInt32(pageContent.Take(1));
            pf_ph.PPageData = pageContent.Remove(0, 1);
        }

        //
        // IsValidPageNum
        //
        // Desc: Internal.  Return TRUE if pageNum is a valid page number
        //       in the file, FALSE otherwise
        // In:   pageNum - page number to test
        // Ret:  TRUE or FALSE
        //
        private bool IsValidPageNum(int pageNum)
        {
            if (bFileOpen && pageNum > 0 && pageNum < pf_bm.NumPages)
                return true;
            return false;
        }
    }
}
