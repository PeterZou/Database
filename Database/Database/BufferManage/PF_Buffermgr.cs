using Database.Const;
using Database.IO;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using log4net;
using log4net.Config;
using System.Reflection;
using Database.FileManage;
using Database.Util;

namespace Database.BufferManage
{
    public class PF_Buffermgr
    {
        public List<PF_BufPageDesc> freeList;
        public List<PF_BufPageDesc> usedList;
        private PF_Hashtable pf_h;
        public int NumPages { get; set; }
        private int pageSize;
        private ILog m_log;
        public FileStream fs;

        /// <summary>
        /// consist of NumPages pages, and PF_HASH_TBL_SIZE hashentries
        /// </summary>
        /// <param name="numPages"></param>
        public PF_Buffermgr(int numPages)
        {
            usedList = new List<PF_BufPageDesc>();
            freeList = new List<PF_BufPageDesc>();

            pf_h = new PF_Hashtable(ConstProperty.PF_HASH_TBL_SIZE);
            this.NumPages = numPages;
            this.pageSize = ConstProperty.PF_PAGE_SIZE + ConstProperty.PF_PageHdr_SIZE;
            for (int i = 0; i < this.NumPages; i++)
            {
                PF_BufPageDesc temp = new PF_BufPageDesc();
                temp.slotSequence = i;
                freeList.Add(temp);
            }

            Type type = MethodBase.GetCurrentMethod().DeclaringType;
            m_log = LogManager.GetLogger(type);
        }

        //
        // GetPage
        //
        // Desc: Get a pointer to a page pinned in the buffer.  If the page is
        //       already in the buffer, (re)pin the page and return a pointer
        //       to it.  If the page is not in the buffer, read it from the file,
        //       pin it, and return a pointer to it.  If the buffer is full,
        //       replace an unpinned page.
        // In:   fd - OS file descriptor of the file to read
        //       pageNum - number of the page to read
        //       bMultiplePins - if FALSE, it is an error to ask for a page that is
        //                       already pinned in the buffer.
        // Out:  ppBuffer - set *ppBuffer to point to the page in the buffer
        // Ret:  PF return code
        // TODO
        public PF_BufPageDesc GetPage(int fd, int pageNum,
              bool bMultiplePins)
        {
            char[] outputResult;
            int slot = pf_h.Found(fd, pageNum);
            // If page not in buffer...
            if (slot == -1)
            {
                slot = InternalAlloc(pageNum);
                outputResult = FileUtil.ReadPage(fd, pageNum,pageSize,fs);
                pf_h.Insert(fd, pageNum, slot);
                InitPageDesc(fd, pageNum, slot);

                var node = usedList[0];
                node.data = outputResult;
                usedList[0] = node;
            }
            else
            {
                var node = usedList.Find(n => n.slotSequence == slot);
                int index = usedList.IndexOf(node);
                // Error if we don't want to get a pinned page
                // TODO
                if (!bMultiplePins && node.pinCount > 0) throw new Exception();
                node.pinCount++;
                // make the most recently used
                usedList.RemoveAt(index);
                usedList.Insert(0, node);
            }
            return usedList[0];
            
        }

        //
        // AllocatePage
        //
        // Desc: Allocate a new page in the buffer and return a pointer to it.
        // In:   fd - OS file descriptor of the file associated with the new page
        //       pageNum - number of the new page
        // Out:  ppBuffer - set *ppBuffer to point to the page in the buffer
        // Ret:  PF return code
        // TODO
        public PF_BufPageDesc AllocatePage(int fd, int pageNum)
        {
            int slot = pf_h.Found(fd, pageNum);
            //slot must be -1
            if (slot != -1) throw new Exception();
            slot = InternalAlloc(pageNum);
            pf_h.Insert(fd, pageNum, slot);
            InitPageDesc(fd, pageNum, slot);
            return usedList[0];
        }

        //
        // MarkDirty
        //
        // Desc: Mark a page dirty so that when it is discarded from the buffer
        //       it will be written back to the file.
        // In:   fd - OS file descriptor of the file associated with the page
        //       pageNum - number of the page to mark dirty
        // Ret:  PF return code
        //
        public void MarkDirty(int fd, int pageNum)
        {
            int slot = pf_h.Found(fd, pageNum);

            var node = usedList.Find(n => n.slotSequence == slot);

            if (node.pinCount == 0) throw new Exception();

            int index = usedList.IndexOf(node);

            node.dirty = true;

            usedList.RemoveAt(index);
            usedList.Insert(0, node);
        }

        //
        // UnpinPage
        //
        // Desc: Unpin a page so that it can be discarded from the buffer.
        // In:   fd - OS file descriptor of the file associated with the page
        //       pageNum - number of the page to unpin
        // Ret:  PF return code
        //
        public void UnpinPage(int fd, int pageNum)
        {
            int slot = pf_h.Found(fd, pageNum);

            var node = usedList.Find(n => n.slotSequence == slot);

            if (node.fd == 0 || node.pinCount == 0) throw new Exception();

            // If unpinning the last pin, make it the most recently used page
            node.pinCount--;

            int index = usedList.IndexOf(node);
            usedList[index] = node;

            if (node.pinCount == 0)
            {
                usedList.RemoveAt(index);
                usedList.Insert(0, node);
            }
        }

        //
        // FlushPages
        //
        // Desc: Release all pages for this file and put them onto the free list
        //       Returns a warning if any of the file's pages are pinned.
        //       A linear search of the buffer is performed.
        //       A better method is not needed because # of buffers are small.
        // In:   fd - file descriptor
        // Ret:  PF_PAGEPINNED or other PF return code
        //
        public void FlushPages(int fd)
        {
            List<int> indexArray = new List<int>();
            PF_BufPageDesc headNode;
            foreach(var u in usedList)
            {
                if (u.fd == fd)
                {
                    if (u.pinCount != 0)
                    {
                        // Ensure pinCount is 0
                        // TODO
                        m_log.Warn(u.ToString());
                    }
                    else
                    {
                        int index = usedList.IndexOf(u);
                        if (u.dirty)
                        {
                            FileUtil.WritePage(u.fd, u.pageNum, u.data,pageSize,fs);
                            headNode = u;
                            headNode.dirty = false;
                        }
                        else
                        {
                            headNode = u;
                        }

                        pf_h.Delete(fd, u.pageNum);
                        indexArray.Add(index);
                        freeList.Insert(0, headNode);
                    }
                }
            }
            indexArray.Reverse();
            foreach (var i in indexArray)
                usedList.RemoveAt(i);
        }

        //
        // ForcePages
        //
        // Desc: If a page is dirty then force the page from the buffer pool
        //       onto disk.  The page will not be forced out of the buffer pool.
        // In:   The page number, a default value of ALL_PAGES will be used if
        //       the client doesn't provide a value.  This will force all pages.
        //       ALL_PAGES refer to 100000 tempory
        // Ret:  Standard PF errors
        //
        //
        public void ForcePages(int fd, int pageNum)
        {
            List<int> indexArray = new List<int>();
            PF_BufPageDesc headNode;
            foreach (var u in usedList)
            {
                if (u.fd == fd && (pageNum == u.pageNum || pageNum == ConstProperty.ALL_PAGES))
                {
                    if (u.dirty)
                    {
                        int index = usedList.IndexOf(u);
                        FileUtil.WritePage(u.fd, u.pageNum, u.data,pageSize,fs);
                        headNode = u;
                        headNode.dirty = false;
                        indexArray.Add(index);
                        usedList.Insert(index, headNode);
                    }
                }
            }
            indexArray.Reverse();
            foreach (var i in indexArray)
                usedList.RemoveAt(i);
        }

        //
        // InternalAlloc
        //
        // Desc: Internal.  Allocate a buffer slot.  The slot is inserted at the
        //       head of the used list.  Here's how it chooses which slot to use:
        //       If there is something on the free list, then use it.
        //       Otherwise, choose a victim to replace.  If a victim cannot be
        //       chosen (because all the pages are pinned), then return an error.
        // Out:  slot - set to newly-allocated slot
        // Ret:  PF_NOBUF if all pages are pinned, other PF return code otherwise
        private int InternalAlloc(int pageNum)
        {
            int slot = -1;
            PF_BufPageDesc headNode = new PF_BufPageDesc();

            // If the free list is not empty, choose a slot from the free list
            if (freeList.Count > 0)
            {
                slot = freeList[0].slotSequence;
                headNode = freeList[0];
                freeList.RemoveAt(0);
            }
            else
            {
                int tempNum = -1;

                //If not, select the least recently used node from the usedList
                for (int i = usedList.Count - 1; i >= 0; i--)
                {
                    if (usedList[i].pinCount == 0)
                    {
                        tempNum = i;
                        slot = usedList[i].slotSequence;
                        break;
                    }
                }

                if (slot == -1) throw new Exception();

                headNode = usedList[tempNum];
                // Write out the page if it is dirty
                if (headNode.dirty == true)
                {
                    FileUtil.ReplaceTheNextFree(headNode.data,pageNum,0);
                    FileUtil.WritePage(headNode.fd, headNode.pageNum
                        , headNode.data,pageSize,fs);
                    headNode.dirty = false;
                }

                // Remove page from the hash table and slot from the used buffer list
                pf_h.Delete(headNode.fd, headNode.pageNum);
                usedList.RemoveAt(tempNum);
            }
            // Make it to the most recently usedList
            usedList.Insert(0, headNode);
            return slot;
        }

        /// <summary>
        /// Init the page desc entry 
        /// And already allocated
        /// It must be the first node in the usedlist
        /// </summary>
        /// <param name="fd"></param>
        /// <param name="pageNum"></param>
        /// <param name="slot"></param>
        private void InitPageDesc(int fd, int pageNum, int slot)
        {
            //The node allocated recently
            var node = usedList[0];
            node.fd = fd;
            node.pageNum = pageNum;
            node.pinCount = 1;
            node.dirty = false;
            usedList[0] = node;
        }

        
    }
}
