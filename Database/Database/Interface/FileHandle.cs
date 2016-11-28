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
    public abstract class FileHandle
    {
        public PF_FileHandle pfHandle;              // pointer to opened PF_FileHandle
        public bool bFileOpen;                        // file open flag
        public bool bHdrChanged;                      // dirty flag for file hdr

        public int GetNumPages() { return hdr.numPages; }

        public RM_PageHdr pHdr;

        public Tuple<RID, PF_PageHandle> GetNextFreeSlot()
        {
            IsValid();

            var tmp = GetNextFreePage();
            int pageNum = tmp.Item1;
            pHdr = (RM_PageHdr)tmp.Item2;

            PF_PageHandle ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);

            int slotNum = GetNumSlots();
            var bitmap = new Bitmap(pHdr.freeSlotMap, slotNum);

            for (UInt32 i = 0; i < slotNum; i++)
            {
                if (bitmap.Test(i))
                {
                    return new Tuple<RID, PF_PageHandle>(new RID(pageNum, (int)i), ph);
                }
            }

            throw new Exception();
        }

        public int GetNumSlots()
        {
            if (fullRecordSize() <= 0) throw new Exception();

            int bytes_available = ConstProperty.PF_PAGE_SIZE
                - 2 * ConstProperty.PF_FILE_HDR_NumPages_SIZE;

            var slots = (int)Math.Floor(1.0 * bytes_available / (fullRecordSize()));

            // 每个UTF占用三个字节
            int r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars() * 3;

            while (slots * fullRecordSize() + r > ConstProperty.PF_PAGE_SIZE + ConstProperty.PF_FILE_HDR_NumPages_SIZE)
            {
                slots--;
                r = ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap + (new Bitmap(slots)).numChars() * 3;
            }

            return slots;
        }

        public RM_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            if (pHdr == null) pHdr = new RM_PageHdr();

            char[] buf = ph.pPageData;
            pHdr.From_buf(buf);
            return pHdr;
        }

        public void SetPageHeader(PF_PageHandle ph, RM_PageHdr pHdr)
        {
            // Just replace the head
            char[] header = pHdr.To_buf();
            for (int i = 0; i < header.Length; i++) ph.pPageData[i] = header[i];

            pfHandle.SetThisPage(ph);
        }

        bool hdrChanged() { return bHdrChanged; }

        public void ForcePages(int pageNum)
        {
            IsValid();
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

        public void IsValid()
        {
            if ((pfHandle == null) || !bFileOpen || GetNumSlots() <= 0) throw new Exception();
        }

        public char[] GetSlotPointer(PF_PageHandle ph, int slot)
        {
            int offset = CalcOffset(slot);
            int offsetAfter = offset + fullRecordSize();
            int index = 0;
            char[] data = new char[fullRecordSize()];
            for (int i = offset; i < offsetAfter; i++)
            {
                data[index] = ph.pPageData[i];
                index++;
            }
            return data;
        }

        protected void SetSlotPointer(PF_PageHandle ph, int slot, char[] data)
        {
            int offset = CalcOffset(slot);

            // Replace
            for (int i = 0; i < data.Length; i++)
            {
                ph.pPageData[offset + i] = data[i];
            }
        }

        private int CalcOffset(int slot)
        {
            IsValid();
            var bitmap = new Bitmap(GetNumSlots());
            int offset = (new RM_PageHdr(GetNumSlots(), new PF_PageHdr())).Size();
            offset += slot * fullRecordSize();

            return offset;
        }

        //
        // IsValidRID
        //
        //
        public bool IsValidRID(RID rid)
        {
            int page = rid.Page;
            int slot = rid.Slot;

            return IsValidPageNum(page) && slot >= 0 && slot < GetNumSlots();
        }

        abstract public int fullRecordSize();

        abstract public RID InsertRec(char[] pData);

        abstract public void DeleteRec(RID rid);

        abstract public void UpdateRec(RM_Record rec);

        abstract public void SetFileHeader(PF_PageHandle ph);

        abstract public Tuple<int, RM_PageHdr> GetNextFreePage();
    }
}
