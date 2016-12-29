using Database.Const;
using Database.FileManage;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.Interface
{
    // if the page is arbitrary, we can use a number like -1;
    public abstract class FileHandle
    {
        public PF_FileHandle pfHandle;              // pointer to opened PF_FileHandle
        public bool bFileOpen;                        // file open flag
        public bool bHdrChanged;                      // dirty flag for file hdr

        public abstract int fullRecordSize(int size);

        public int GetNumSlots(int size)
        {
            int num = fullRecordSize(size);

            if (num <= 0) throw new Exception();

            int bytes_available = ConstProperty.PF_PAGE_SIZE
                - 2 * ConstProperty.PF_FILE_HDR_NumPages_SIZE;

            var slots = (int)Math.Floor(1.0 * bytes_available / (num));

            // 每个UTF占用三个字节
            int r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars() * 3;

            while (slots * num + r > ConstProperty.PF_PAGE_SIZE + ConstProperty.PF_FILE_HDR_NumPages_SIZE)
            {
                slots--;
                r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars() * 3;
            }

            return slots;
        }

        public void SetPageHeader(PF_PageHandle ph, PageHdr pHdr)
        {
            // Just replace the head
            char[] header = pHdr.To_buf();
            for (int i = 0; i < header.Length; i++) ph.pPageData[i] = header[i];

            pfHandle.SetThisPage(ph);
        }

        bool hdrChanged() { return bHdrChanged; }

        public void ForcePages(int pageNum)
        {
            IsValid(-1);
            if (!IsValidPageNum(pageNum) && pageNum != ConstProperty.ALL_PAGES) throw new Exception();
            pfHandle.ForcePages(pageNum);
        }

        //
        // IsValidPageNum
        //
        // Desc: Internal.  Return TRUE if pageNum is a valid page number
        //       in the file, FALSE otherwise
        // In:   pageNum - page number to test
        // Ret:  TRUE or FALSE
        //
        protected bool IsValidPageNum(int pageNum)
        {
            return pfHandle.IsValidPageNum(pageNum);
        }

        public char[] GetSlotPointer(PF_PageHandle ph, int slot,int size)
        {
            int offset = CalcOffset(slot,size);
            int index = 0;
            char[] data = new char[fullRecordSize(size)];
            for (int i = offset; i < fullRecordSize(size)+ offset; i++)
            {
                data[index] = ph.pPageData[i];
                index++;
            }
            return data;
        }

        protected void SetSlotPointer(PF_PageHandle ph, int slot, char[] data,int childNum)
        {
            int offset = CalcOffset(slot, childNum);

            // Replace
            for (int i = 0; i < data.Length; i++)
            {
                ph.pPageData[offset + i] = data[i];
            }
        }

        protected void SetSlotPointer(PF_PageHandle ph, int slot, char[] data)
        {
            SetSlotPointer(ph, slot, data, -1);
        }

        //
        // IsValidRID
        //
        //
        public bool IsValidRID(RID rid)
        {
            int page = rid.Page;
            int slot = rid.Slot;

            return IsValidPageNum(page) && slot >= 0;
        }

        abstract public RID InsertRec(char[] pData);

        abstract public void SetFileHeader(PF_PageHandle ph);

        abstract public int CalcOffset(int slot, int size);

        abstract public void IsValid(int size);
    }
}
