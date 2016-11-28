using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.Interface;

namespace Database.RecordManage
{
    public class RM_FileHandle : FileHandle
    {
        public RM_FileHdr hdr;                        // file header
        public RM_PageHdr pHdr;

        #region Unique
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

        public Tuple<int, RM_PageHdr> GetNextFreePage()
        {
            int pageNum = -3;
            IsValid();
            PF_PageHandle ph;
            int slotNum = GetNumSlots();

            if (pHdr == null)
            {
                pHdr = new RM_PageHdr(slotNum, new PF_PageHdr());
            }


            // 如果有页面内部还剩余slot没有满的，该页应该做为freepage留给系统可以继续分配
            // QA: the meaning of this branch and what is pHdr in original refer to?
            // QA2: key point is how to define the free page? if there is still freeslot, is it a free page?
            if (hdr.firstFree != (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {
                // 11252016 对应的是新打开的文件，pHdr还没有读入，或者页面发生改变(由于换页)
                ph = pfHandle.GetThisPage(hdr.firstFree);
                if (pHdr.numSlots == pHdr.numFreeSlots || pHdr.numFreeSlots != GetPageHeader(ph).numFreeSlots)
                {
                    pHdr = GetPageHeader(ph);
                }
                pageNum = ph.pageNum;
                pfHandle.MarkDirty(pageNum);
                pfHandle.UnpinPage(pageNum);
            }
            else if (hdr.firstFree == (int)ConstProperty.Page_statics.PF_PAGE_LIST_END || pHdr.numFreeSlots == 0)
            {
                pfHandle.hdr.firstFree = hdr.firstFree;
                pfHandle.hdr.numPages = hdr.numPages;
                ph = pfHandle.AllocatePage();
                pageNum = ph.pageNum;
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
                var bitmap = new Bitmap(GetNumSlots());
                bitmap.Reset(); // Initially all slots are free
                pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());

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

            return new Tuple<int, RM_PageHdr>(pageNum, pHdr);
        }

        public Tuple<RID, PF_PageHandle> GetNextFreeSlot()
        {
            IsValid();

            var tmp = GetNextFreePage();
            int pageNum = tmp.Item1;
            pHdr = tmp.Item2;

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

        public int fullRecordSize() { return hdr.extRecordSize; }
        #endregion

        override public int GetNumPages() { return hdr.numPages; }

        override public void IsValid()
        {
            if ((pfHandle == null) || !bFileOpen || GetNumSlots() <= 0) throw new Exception();
        }

        override public RM_Record GetRec(RID rid)
        {
            IsValid();
            if (!IsValidRID(rid)) throw new Exception();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            PF_PageHandle ph;
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

        override public RID InsertRec(char[] pData)
        {
            IsValid();
            // TODO:consider about the last '\0' of a string
            if (pData == null || pData.Length == 0 || pData.Length > fullRecordSize()) throw new Exception();

            var tuple = GetNextFreeSlot();

            PF_PageHandle ph = tuple.Item2;
            RID rid = tuple.Item1;
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());

            if (pHdr.numFreeSlots == 0)
            {
                pHdr.numFreeSlots = GetNumSlots();
            }

            SetSlotPointer(ph, slotNum, pData);

            bitmap.Set((UInt32)slotNum); // slot s is no longer free
            pHdr.numFreeSlots--;
            if (pHdr.numFreeSlots == 0)
            {
                // remove from free list 
                hdr.firstFree = pHdr.pf_ph.nextFree;
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_USED;
            }
            pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());
            SetPageHeader(ph, pHdr);
            
            return rid;
        }

        override public void UpdateRec(RM_Record rec)
        {
            IsValid();
            RID rid = rec.GetRid();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            if (!IsValidRID(rid)) throw new Exception();

            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
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

        override public void DeleteRec(RID rid)
        {
            IsValid();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots());
            bitmap.Reset((UInt32)slotNum);

            if (pHdr.numFreeSlots == 0)
            {
                pHdr.pf_ph.nextFree = hdr.firstFree;
                hdr.firstFree = pageNum;
            }
            pHdr.numFreeSlots++;
            pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());
            SetPageHeader(ph, pHdr);
        }

        override public void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = RecordManagerUtil.SetFileHeaderToChar(hdr);
        }

        private char[] GetSlotPointer(PF_PageHandle ph, int slot)
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

        private void SetSlotPointer(PF_PageHandle ph, int slot, char[] data)
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
        private bool IsValidRID(RID rid)
        {
            int page = rid.Page;
            int slot = rid.Slot;

            return IsValidPageNum(page) && slot >= 0 && slot < GetNumSlots();
        }
    }
}
