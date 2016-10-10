using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

namespace Database.RecordManage
{
    public class RM_FileHandle
    {
        private PF_FileHandle pfHandle;              // pointer to opened PF_FileHandle
        private RM_FileHdr hdr;                        // file header
        private bool bFileOpen;                        // file open flag
        private bool bHdrChanged;                      // dirty flag for file hdr

        // You will probably then want to copy the file header information into a
        // private variable in the file handle that refers to the open file instance. By
        // copying this information, you will subsequently be able to find out details
        // such as the record size and the number of pages in the file by looking in the
        // file handle instead of reading the header page again (or keeping the
        // information on every page).
        public RM_FileHandle()
        {
            bFileOpen = false;
            bHdrChanged = false;

        }

        bool hdrChanged() { return bHdrChanged; }

        int fullRecordSize() { return hdr.extRecordSize; }

        int GetNumPages() { return hdr.pf_fh.numPages; }

        public int GetNumSlots()
        {
            if (fullRecordSize() <= 0) throw new Exception();

            int bytes_available = ConstProperty.PF_PAGE_SIZE 
                - ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap;

            var slots = (int)Math.Floor(1.0 * bytes_available / (fullRecordSize() + 1 / 8));

            int r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars();

            while (slots * fullRecordSize() + r > ConstProperty.PF_PAGE_SIZE)
            {
                slots--;
                r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars();
            }

            return slots;
        }

        public void IsValid()
        {
            if ((pfHandle == null) || !bFileOpen || GetNumSlots() <= 0) throw new Exception();
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
            return pfHandle.IsValidPageNum(pageNum);
        }

        //
        // IsValidRID
        //
        //
        private bool IsValidRID(RID rid)
        {
            int page = rid.Page;
            int slot = rid.Slot;

            return IsValidPageNum(page) && slot >= 0 && slot < GetNumSlots();
        }

        /// <summary>
        /// TODO
        /// </summary>
        /// <returns></returns>
        public int GetNextFreePage()
        {
            int pageNum = -3;
            IsValid();
            PF_PageHandle ph;
            RM_PageHdr pHdr = new RM_PageHdr(GetNumSlots(), new PF_PageHdr());

            // QA: the meaning of this branch and what is pHdr in original refer to?
            if (hdr.pf_fh.firstFree != (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {

            }
            else if (hdr.pf_fh.firstFree == (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            { }
            else
                throw new Exception();

            return 0;
        }

        private RM_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            var pHdr = new RM_PageHdr();
            char[] buf = ph.pPageData;
            pHdr.From_buf(buf);
            return pHdr;
        }

        private void SetPageHeader(PF_PageHandle ph, RM_PageHdr phdr)
        {
            var pHdr = new RM_PageHdr();
            // Just replace the head
            char[] header = pHdr.To_buf();
            for (int i = 0; i < header.Length; i++) ph.pPageData[i] = header[i];
        }

        private void GetFileHeader(PF_PageHandle ph)
        {
            string str = new string(ph.pPageData);
            Int32.TryParse(str.Substring
                (0,ConstProperty.PF_PageHdr_SIZE)
                , out hdr.pf_fh.firstFree);
            Int32.TryParse(str.Substring
                (ConstProperty.PF_PageHdr_SIZE
                , 2*ConstProperty.PF_PageHdr_SIZE)
                , out hdr.pf_fh.numPages);
            Int32.TryParse(str.Substring
                (2*ConstProperty.PF_PageHdr_SIZE
                , 3 * ConstProperty.PF_PageHdr_SIZE), out hdr.extRecordSize);
        }

        private void SetFileHeader(PF_PageHandle ph)
        {
            char[] content = new char[3* ConstProperty.PF_PageHdr_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.pf_fh.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.pf_fh.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            ph.pPageData = content;
        }

        // TODO
        private char[] GetSlotPointer(PF_PageHandle ph, int slot)
        {
            return null;
        }
    }
}
