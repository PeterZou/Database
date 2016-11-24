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
        public PF_FileHandle pfHandle;              // pointer to opened PF_FileHandle
        public RM_FileHdr hdr;                        // file header
        public bool bFileOpen;                        // file open flag
        public bool bHdrChanged;                      // dirty flag for file hdr

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

        public int fullRecordSize() { return hdr.extRecordSize; }

        public int GetNumPages() { return hdr.numPages; }

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

        public void Open(PF_FileHandle pfh, RM_FileHdr hdr_tmp)
        {
            if (bFileOpen || pfHandle != null) throw new Exception();

            if (pfh == null) throw new Exception();

            bFileOpen = true;
            pfHandle = pfh;

            hdr = hdr_tmp;

            if (hdr.extRecordSize <= 0) throw new Exception();

            bHdrChanged = true;

            IsValid();
        }

        public void ForcePages(int pageNum)
        {
            IsValid();
            if (!IsValidPageNum(pageNum) && pageNum != ConstProperty.ALL_PAGES) throw new Exception();
            pfHandle.ForcePages(pageNum);
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
            // QA2: key point is how to define the free page? if there is still freeslot, is it a free page?
            if (hdr.firstFree != (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {
                ph = pfHandle.GetThisPage(hdr.firstFree);
                pageNum = ph.pageNum;
                pfHandle.MarkDirty(pageNum);
                pfHandle.UnpinPage(pageNum);
                pHdr = GetPageHeader(ph);

            }
            if (hdr.firstFree == (int)ConstProperty.Page_statics.PF_PAGE_LIST_END || pHdr.numFreeSlots == 0)
            {
                ph = pfHandle.AllocatePage();
                pageNum = ph.pageNum;
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
                var bitmap = new Bitmap(GetNumSlots());
                bitmap.Set(); // Initially all slots are free
                bitmap.To_char_buf(pHdr.freeSlotMap, bitmap.numChars());

                // TODO what is the use of ph.pPageData? no need to write into the disk?
                ph.pPageData = pHdr.To_buf();

                pfHandle.UnpinPage(pageNum);

                // add page to the free list
                hdr.firstFree = pageNum;
                hdr.numPages++;
                bHdrChanged = true;

            }
            else
                throw new Exception();

            return pageNum;
        }

        public Tuple<RID,PF_PageHandle> GetNextFreeSlot()
        {
            IsValid();

            int pageNum = GetNextFreePage();

            PF_PageHandle ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);

            var pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());
            for (UInt32 i = 0; i < GetNumSlots(); i++)
            {
                if (bitmap.Test(i))
                {
                    return new Tuple<RID, PF_PageHandle>(new RID(pageNum, (int)i), ph);
                }
            }

            throw new Exception();
        }

        public RM_Record GetRec(RID rid)
        {
            IsValid();
            if (!IsValidRID(rid)) throw new Exception();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            PF_PageHandle ph;
            RM_PageHdr pHdr;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());

            // already free
            if (bitmap.Test((UInt32)slotNum)) throw new Exception();

            char[] data = GetSlotPointer(ph, slotNum);

            var rec = new RM_Record();
            rec.Set(data, hdr.extRecordSize, rid);
            return rec;
        }

        public RID InsertRec(char[] pData)
        {
            IsValid();
            // TODO:consider about the last '\0' of a string
            if (pData == null || pData.Length == 0 || pData.Length > fullRecordSize()) throw new Exception();

            var tuple = GetNextFreeSlot();

            PF_PageHandle ph = tuple.Item2;
            RID rid = tuple.Item1;
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            RM_PageHdr pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());
            SetSlotPointer(ph, slotNum,ph.pPageData);

            bitmap.Reset((UInt32)slotNum); // slot s is no longer free
            pHdr.numFreeSlots--;
            if (pHdr.numFreeSlots == 0)
            {
                // remove from free list 
                hdr.firstFree = pHdr.pf_ph.nextFree;
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
            }
            bitmap.To_char_buf(pHdr.freeSlotMap, bitmap.numChars());

            SetPageHeader(ph, pHdr);
            return rid;
        }

        public void UpdateRec(RM_Record rec)
        {
            IsValid();
            RID rid = rec.GetRid();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            if (!IsValidRID(rid)) throw new Exception();

            PF_PageHandle ph;
            RM_PageHdr pHdr;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());

            // already free
            if (bitmap.Test((UInt32)slotNum)) throw new Exception();

            char[] recData = rec.GetData();
            if (recData.Length != fullRecordSize()) throw new Exception();

            SetSlotPointer(ph, slotNum, recData);

            SetPageHeader(ph, pHdr);
        }

        public void DeleteRec(RID rid)
        {
            IsValid();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
            pfHandle.UnpinPage(pageNum);
            RM_PageHdr pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());
            bitmap.Set((UInt32)slotNum);

            if (pHdr.numFreeSlots == 0)
            {
                hdr.firstFree = pHdr.pf_ph.nextFree;
                pHdr.pf_ph.nextFree = pageNum;
            }
            pHdr.numFreeSlots++;
            bitmap.To_char_buf(pHdr.freeSlotMap, bitmap.numChars());

            SetPageHeader(ph, pHdr);
        }

        public RM_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            var pHdr = new RM_PageHdr();
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

        public void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = RecordManagerUtil.SetFileHeaderToChar(hdr);
        }

        private char[] GetSlotPointer(PF_PageHandle ph, int slot)
        {
            IsValid();
            var bitmap = new Bitmap(GetNumSlots());
            int offset = (new RM_PageHdr(GetNumSlots(), new PF_PageHdr())).Size();
            offset += slot * fullRecordSize();
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

        private void SetSlotPointer(PF_PageHandle ph, int slot, char[] data)
        {
            data = GetSlotPointer(ph, slot);
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
    }
}
