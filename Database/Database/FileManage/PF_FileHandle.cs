using Database.BufferManage;
using log4net;
using System;
using System.Linq;
using System.Reflection;
using Database.Const;
using System.IO;

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
        public PF_Buffermgr pf_bm;
        public bool bFileOpen;                                 // file open flag
        private bool bHdrChanged;                              // dirty flag for file hdr
        public int fd;                                        // OS file descriptor
        private FileStream fs;

        public PF_FileHdr hdr;
        private ILog m_log;

        public PF_FileHandle()
        { }

        public PF_FileHandle(PF_FileHdr hdr,string fileName
            , PF_Buffermgr pf_bm,bool bFileOpen, FileStream fs)
        {
            fd = IO.IOFDDic.FDMapping.Where(node => node.Value.Equals(fileName)).Select(node =>node.Key).First();

            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);

            this.pf_bm = pf_bm;
            // Set the FileStream fs
            this.pf_bm.fs = fs;

            this.hdr = hdr;
            this.bFileOpen = bFileOpen;
            this.fs = fs;
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
        public PF_PageHandle GetThisPage(int pageNum)
        {
            if (!(bFileOpen && pageNum >= 0 && pageNum < hdr.numPages)) throw new Exception();

            PF_BufPageDesc pageContent = pf_bm.GetPage(fd, pageNum, true);

            return ReadPage(pageContent, pageNum);
        }

        public void SetThisPage(PF_PageHandle pf_ph)
        {
            var node = pf_bm.usedList.Where(n=>n.pageNum == pf_ph.pageNum).First();

            node.data = pf_ph.pPageData;
        }

        //
        // GetFirstPage
        //
        // Desc: Get the first page in a file
        //       The file handle must refer to an open file
        // Out:  pageHandle - becomes a handle to the first page of the file
        //       The referenced page is pinned in the buffer pool.
        // Ret:  PF return code
        //
        public PF_PageHandle GetFirstPage()
        {
            return GetNextPage(-1);
        }

        //
        // GetLastPage
        //
        // Desc: Get the last page in a file
        //       The file handle must refer to an open file
        // Out:  pageHandle - becomes a handle to the last page of the file
        //       The referenced page is pinned in the buffer pool.
        // Ret:  PF return code
        //
        public PF_PageHandle GetLastPage()
        {
            return GetPrevPage(hdr.numPages - 1);
        }

        //
        // GetNextPage
        //
        // Desc: Get the next (valid) page after current
        //       The file handle must refer to an open file
        // In:   current - get the next valid page after this page number
        //       current can refer to a page that has been disposed
        // Out:  pageHandle - becomes a handle to the next page of the file
        //       The referenced page is pinned in the buffer pool.
        // Ret:  PF_EOF, or another PF return code
        //
        public PF_PageHandle GetNextPage(int pageNum)
        {
            int pageNumThis = pageNum + 1;
            if (!(bFileOpen && pageNumThis >= 0 && pageNumThis < hdr.numPages)) throw new Exception();

            for (pageNum++; pageNum < hdr.numPages; pageNum++)
            {
                // If this is a valid (used) page, we're done
                return GetThisPage(pageNum);
            }

            // No valid (used) page found
            m_log.Warn("This is the last page");
            return null;
        }

        //
        // GetPrevPage
        //
        // Desc: Get the prev (valid) page after current
        //       The file handle must refer to an open file
        // In:   current - get the prev valid page before this page number
        //       current can refer to a page that has been disposed
        // Out:  pageHandle - becomes a handle to the prev page of the file
        //       The referenced page is pinned in the buffer pool.
        // Ret:  PF_EOF, or another PF return code
        //
        public PF_PageHandle GetPrevPage(int pageNum)
        {
            int pageNumThis = pageNum - 1;
            if (!(bFileOpen && pageNumThis >= 0 && pageNumThis < hdr.numPages)) throw new Exception();

            for (pageNum--; pageNum >= 0; pageNum--)
            {
                // If this is a valid (used) page, we're done
                return GetThisPage(pageNum);
            }

            // No valid (used) page found
            m_log.Warn("This is the first page");
            return null;
        }

        //
        // AllocatePage
        //
        // Desc: Allocate a new page in the file (may get a page which was
        //       previously disposed)
        //       The file handle must refer to an open file
        // Out:  pageHandle - becomes a handle to the newly-allocated page
        //                    this function modifies local var's in pageHandle
        // Ret:  PF return code
        //
        public PF_PageHandle AllocatePage()
        {
            int pageNum;

            //page content include the header and handle(num and data) of page
            PF_BufPageDesc content;

            if (!bFileOpen) throw new Exception();

            // If the free list isn't empty...
            if (hdr.firstFree != (Int32)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {
                pageNum = hdr.firstFree;
                content = pf_bm.GetPage(fd, pageNum, false);
                string str = new string(content.data.Take(ConstProperty.PF_PageHdr_SIZE).ToArray());
                Int32.TryParse(str, out hdr.firstFree);
            }
            else
            {
                pageNum = hdr.numPages;
                content = pf_bm.AllocatePage(fd, pageNum);
                hdr.numPages++;
            }

            bHdrChanged = true;

            // replace the pf_ph.nextFree of PF_PAGE_USED in ConstProperty.PF_PageHdr_SIZE chars
            FileManagerUtil.ReplaceTheNextFree(content, (int)ConstProperty.Page_statics.PF_PAGE_USED,0);

            //Is it the same the put the content to the head of the usedlist?
            MarkDirty(pageNum);

            return ReadPageHandleData(content, pageNum);
        }

        //
        // MarkDirty
        //
        // Desc: Mark a page as being dirty
        //       The page will then be written back to disk when it is removed from
        //       the page buffer
        //       The file handle must refer to an open file
        // In:   pageNum - number of page to mark dirty
        // Ret:  PF return code
        //
        public void MarkDirty(int pageNum)
        {
            if (!bFileOpen || !IsValidPageNum(pageNum)) throw new Exception();

            pf_bm.MarkDirty(fd, pageNum);
        }

        //
        // UnpinPage
        //
        // Desc: Unpin a page from the buffer manager.
        //       The page is then free to be written back to disk when necessary.
        //       PF_PageHandle objects referring to this page should not be used
        //       after making this call.
        //       The file handle must refer to an open file.
        // In:   pageNum - number of the page to unpin
        // Ret:  PF return code
        //
        public void UnpinPage(int pageNum)
        {
            if (!bFileOpen || !IsValidPageNum(pageNum)) throw new Exception();

            pf_bm.UnpinPage(fd, pageNum);
        }

        //
        // DisposePage
        //
        // Desc: Dispose of a page
        //       The file handle must refer to an open file
        //       PF_PageHandle objects referring to this page should not be used
        //       after making this call.
        // In:   pageNum - number of page to dispose
        // Ret:  PF return code
        //
        public void DisposePage(int pageNum)
        {
            if (!bFileOpen || !IsValidPageNum(pageNum)) throw new Exception();

            PF_BufPageDesc content = pf_bm.GetPage(fd, pageNum, false);

            int nextFreeTmp = -3;

            var str = new string(content.data.Take(ConstProperty.PF_PageHdr_SIZE-1).ToArray());
            Int32.TryParse(str, out nextFreeTmp);
            if (nextFreeTmp != (int)ConstProperty.Page_statics.PF_PAGE_USED) throw new Exception();

            FileManagerUtil.ReplaceTheNextFree(content.data, hdr.firstFree, 0);
            hdr.firstFree = pageNum;
            //FileManagerUtil.ReplaceTheNextFree(content, hdr.firstFree,0);

            bHdrChanged = true;

            MarkDirty(pageNum);
            UnpinPage(pageNum);
        }

        //
        // FlushPages
        //
        // Desc: Flush all dirty unpinned pages from the buffer manager for this file
        // In:   Nothing
        // Ret:  PF_PAGEFIXED warning from buffer manager if pages are pinned or
        //       other PF error
        //
        public void FlushPages()
        {
            if (!bFileOpen) throw new Exception();

            if (bHdrChanged)
            {
                // write to the filehdr
                FileManagerUtil.WriteFileHdr(hdr,fd,fs);

                bHdrChanged = false;
            }
            pf_bm.FlushPages(fd);
        }

        //
        // ForcePages
        //
        // Desc: If a page is dirty then force the page from the buffer pool
        //       onto disk.  The page will not be forced out of the buffer pool.
        // In:   The page number, a default value of ALL_PAGES will be used if
        //       the client doesn't provide a value.  This will force all pages.
        // Ret:  Standard PF errors
        //
        //
        public void ForcePages(int pageNum)
        {
            if (!bFileOpen) throw new Exception();

            if (bHdrChanged)
            {
                // write to the filehdr
                FileManagerUtil.WriteFileHdr(hdr,fd,fs);

                bHdrChanged = false;
            }
            pf_bm.ForcePages(fd, pageNum);
        }

        private PF_PageHandle ReadPage(PF_BufPageDesc pageContent, int pageNum)
        {
            int pageStatics = -1;
            string str = new string(pageContent.data).Substring(0, ConstProperty.PF_PageHdr_SIZE);
            Int32.TryParse(str, out pageStatics);

            // ConstProperty.Page_statics.PF_PAGE_USED is the situation of the slot be not full
            if (pageStatics == (int)ConstProperty.Page_statics.PF_PAGE_USED || pageStatics == (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {
                return ReadPageHandleData(pageContent, pageNum);
            }
            else
            {
                UnpinPage(pageNum);
                throw new Exception();
            }
        }

        private PF_PageHandle ReadPageHandleData(PF_BufPageDesc pageContent, int pageNum)
        {
            PF_PageHandle currentPage = new PF_PageHandle();

            currentPage.pageNum = pageNum;
            currentPage.pPageData = new string(pageContent.data.Take(ConstProperty.PF_FILE_HDR_SIZE).ToArray()).ToArray();
            m_log.Warn(currentPage.pageNum + " and " + currentPage.pPageData);
            return currentPage;
        }

        //
        // IsValidPageNum
        //
        // Desc: Internal.  Return TRUE if pageNum is a valid page number
        //       in the file, FALSE otherwise
        // In:   pageNum - page number to test
        // Ret:  TRUE or FALSE
        //
        public bool IsValidPageNum(int pageNum)
        {
            if (bFileOpen && pageNum >= 0 && pageNum < hdr.numPages)
                return true;
            return false;
        }
    }
}
