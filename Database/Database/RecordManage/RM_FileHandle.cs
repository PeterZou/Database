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

        override public int fullRecordSize(int size)
        {
            return hdr.extRecordSize;
        }

        override public void DeleteRec(RID rid)
        {
            IsValid(-1);
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(-1));
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

        override public RID InsertRec(char[] pData)
        {
            IsValid(-1);
            // TODO:consider about the last '\0' of a string
            if (pData == null || pData.Length == 0 || pData.Length > fullRecordSize(-1)) throw new Exception();

            var tuple = GetNextFreeSlot();

            PF_PageHandle ph = tuple.Item2;
            RID rid = tuple.Item1;
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(-1));

            if (pHdr.numFreeSlots == 0)
            {
                pHdr.numFreeSlots = GetNumSlots(-1);
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

        override public void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = RecordManagerUtil.SetFileHeaderToChar(hdr);
        }

        public int GetNumPages() { return hdr.numPages; }

        public void Open(PF_FileHandle pfh, RM_FileHdr hdr_tmp)
        {
            if (bFileOpen || pfHandle != null) throw new Exception();

            if (pfh == null) throw new Exception();

            bFileOpen = true;
            pfHandle = pfh;

            hdr = hdr_tmp;

            if (hdr.extRecordSize <= 0) throw new Exception();

            bHdrChanged = true;

            IsValid(-1);
        }

        public Tuple<int, RM_PageHdr> GetNextFreePage()
        {
            int pageNum = -3;
            IsValid(-1);
            PF_PageHandle ph;
            int slotNum = GetNumSlots(-1);

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
                var bitmap = new Bitmap(GetNumSlots(-1));
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

        public void UpdateRec(RM_Record rec)
        {
            IsValid(-1);
            RID rid = rec.GetRid();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            if (!IsValidRID(rid)) throw new Exception();

            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(-1));

            // already free
            if (bitmap.Test((UInt32)slotNum)) throw new Exception();

            char[] recData = rec.GetData();
            if (recData.Length != fullRecordSize(-1)) throw new Exception();

            SetSlotPointer(ph, slotNum, recData);

            SetPageHeader(ph, pHdr);
        }

        public RM_Record GetRec(RID rid)
        {
            IsValid(-1);
            if (!IsValidRID(rid)) throw new Exception();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(-1));

            // already free
            if (bitmap.Test((UInt32)slotNum)) throw new Exception();

            char[] data = GetSlotPointer(ph, slotNum, -1);

            var rec = new RM_Record();
            rec.Set(data, fullRecordSize(-1), rid);
            return rec;
        }

        public Tuple<RID, PF_PageHandle> GetNextFreeSlot()
        {
            IsValid(-1);

            var tmp = GetNextFreePage();
            int pageNum = tmp.Item1;
            pHdr = (RM_PageHdr)tmp.Item2;

            PF_PageHandle ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);

            int slotNum = GetNumSlots(-1);
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

        public RM_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            if (pHdr == null) pHdr = new RM_PageHdr();

            char[] buf = ph.pPageData;
            pHdr.From_buf(buf);
            return pHdr;
        }
    }
}
