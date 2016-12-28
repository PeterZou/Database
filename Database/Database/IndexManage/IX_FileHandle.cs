using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.FileManage;
using Database.RecordManage;
using Database.Const;
using Database.Interface;
using Database.IndexManage.IndexValue;
using Database.IndexManage.BPlusTree;

namespace Database.IndexManage
{
    // 11282016 抽象FileHandle主要是因为Index保存的主要是非固定数据
    // 非固定情况下，page的选择策略：
    // 主要问题：1.由于可以删除，这时候插入与删除大小不一致的数据时，1）插不进去2）插进去，但是会很多剩余的间隙
    // 策略：
    //       1）bitMap,预先设置一个定值，随着插入和删除动态的增加和删减
    //       2）data,尽可能在一页插入数据，1)遍历后发现所有的slot都比较小，做gc,压缩
    //                                   2)压缩之后发现还是不够插入，①保留压缩后的data为以后插入做准备，②循环进入下一页重复
    // 2.将非固定问题转化为特定几个固定的长度，比如degree为5，那么长度应该为Fixlen + n*Fixlen2(0<=n<=degress)，全部放在一个文件中，由文件表头定义(N+1)个freelist
    // Point:如果遇到allocate,需要添加额外的操作保证对firstpage的操作转化为对dic的操作Wrapper
    public class IX_FileHandle<TK> : FileHandle
        where TK : IComparable<TK>
    {
        public IX_IndexHandle<TK> iih;

        public IX_FileHdr<TK> hdr;                        // file header
        public IX_PageHdr pHdr;
        public Func<TK, string> ConverTKToString;

        public IX_FileHandle(PF_FileHandle pfh, Func<TK, string> converTKToString)
        {
            bFileOpen = false;
            bHdrChanged = false;
            pfHandle = pfh;
            this.ConverTKToString = converTKToString;
        }

        public IX_FileHandle(IX_IndexHandle<TK> iih, PF_FileHandle pfh, Func<TK, string> converTKToString)
        {
            bFileOpen = false;
            bHdrChanged = false;
            this.iih = iih;
            pfHandle = pfh;
            this.ConverTKToString = converTKToString;
        }

        override public void DeleteRec(RID rid)
        {
            int pageNum = rid.Page;
            int slotNum = rid.Slot;
            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.MarkDirty(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);
            int size = pHdr.size;
            IsValid(pageNum);

            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(size));
            bitmap.Reset((UInt32)slotNum);

            if (pHdr.numFreeSlots == 0)
            {
                pHdr.pf_ph.nextFree = hdr.dic[size];
                hdr.dic[size] = pageNum;
                //pHdr.pf_ph.nextFree = hdr.firstFree;
                //hdr.firstFree = pageNum;
            }
            pHdr.numFreeSlots++;
            pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());
            SetPageHeader(ph, pHdr);
        }

        override public void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = IndexManagerUtil<TK>.IndexFileHdrToCharArray(hdr, iih.ConverTKToString);
        }

        override public int fullRecordSize(int size)
        {
            // TODO defalut TK occupied 4 Bytes
            return 4 * ConstProperty.Int_Size + ConstProperty.Int_Size + ConstProperty.RM_Page_RID_SIZE;
        }

        override public RID InsertRec(char[] pData)
        {
            // Set the child and value num in the pData 
            int size = CalculateSize(pData.Length,pData[4]-48);

            IsValid(size);

            if (pData == null || pData.Length == 0) throw new Exception();

            var tuple = GetNextFreeSlot(size);

            PF_PageHandle ph = tuple.Item2;
            RID rid = tuple.Item1;
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(size));

            if (pHdr.numFreeSlots == 0)
            {
                pHdr.numFreeSlots = GetNumSlots(size);
            }

            SetSlotPointer(ph, slotNum, pData, size);

            bitmap.Set((UInt32)slotNum); // slot s is no longer free
            pHdr.numFreeSlots--;
            if (pHdr.numFreeSlots == 0)
            {
                hdr.dic[size] = pHdr.pf_ph.nextFree;
                hdr.firstFree = hdr.dic[size];
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_USED;
            }
            pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());
            SetPageHeader(ph, pHdr);

            return rid;
        }

        override public int CalcOffset(int slot, int size)
        {
            IsValid(size);
            int offset = (new IX_PageHdr(GetNumSlots(size), new PF_PageHdr(),size)).Size();
            offset += slot * fullRecordSize(size);

            return offset;
        }

        public void ShowPartitialBplustree(RID rid)
        {
            //TODO
        }

        public void DeleteEntry(TK key)
        {
            iih.DeleteEntry(key);
        }

        public void InsertEntry(TK key)
        {
            // need to replace
            RIDKey<TK> value = new RIDKey<TK>(new RID(-2,-2),key);
            iih.InsertEntry(value);
            if (iih.ChangeOrNot == true)
            {
                bHdrChanged = true;
                iih.ChangeOrNot = false;
            }
        }

        public RM_Record GetRec(RID rid)
        {
            if (!IsValidRID(rid)) throw new Exception();
            int pageNum = rid.Page;
            int slotNum = rid.Slot;

            PF_PageHandle ph;
            ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);
            pHdr = GetPageHeader(ph);

            IsValid(pageNum);
            var bitmap = new Bitmap(pHdr.freeSlotMap, GetNumSlots(pHdr.size));

            // already free
            if (bitmap.Test((UInt32)slotNum)) throw new Exception();

            char[] data = GetSlotPointer(ph, slotNum, pHdr.size);

            var rec = new RM_Record();
            rec.Set(data, fullRecordSize(pHdr.size), rid);
            return rec;
        }

        public int GetNumPages() { return hdr.numPages; }

        public Tuple<int, IX_PageHdr> GetNextFreePage(int degreesSize)
        {
            int pageNum = -3;
            PF_PageHandle ph;

            int slotNum = GetNumSlots(degreesSize);

            if (pHdr == null)
            {
                pHdr = new IX_PageHdr(slotNum, new PF_PageHdr(), degreesSize);
            }

            // 对应的
            if (hdr.dic[degreesSize] != (int)ConstProperty.Page_statics.PF_PAGE_LIST_END)
            {
                ph = pfHandle.GetThisPage(hdr.dic[degreesSize]);
                if (pHdr.numSlots == pHdr.numFreeSlots || pHdr.numFreeSlots != GetPageHeader(ph).numFreeSlots)
                {
                    pHdr = GetPageHeader(ph);
                }
                pageNum = ph.pageNum;
                pfHandle.MarkDirty(pageNum);
                pfHandle.UnpinPage(pageNum);
            }
            else if (hdr.dic[degreesSize] == (int)ConstProperty.Page_statics.PF_PAGE_LIST_END || pHdr.numFreeSlots == 0)
            {
                pfHandle.hdr.numPages = hdr.numPages;

                #region Wrap the dic put into the firstFree and then replace it
                pfHandle.hdr.firstFree = hdr.dic[degreesSize];
                ph = pfHandle.AllocatePage();
                //hdr.dic[degreesSize] = pfHandle.hdr.firstFree;
                #endregion

                pageNum = ph.pageNum;
                pHdr.pf_ph.nextFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
                pHdr.size = degreesSize;
                var bitmap = new Bitmap(GetNumSlots(pageNum));
                bitmap.Reset();
                pHdr.freeSlotMap = bitmap.To_char_buf(bitmap.numChars());

                ph.pPageData = pHdr.To_buf();

                pfHandle.UnpinPage(pageNum);

                hdr.dic[degreesSize] = pageNum;
                hdr.numPages++;
                bHdrChanged = true;
            }
            else
                throw new Exception();

            return new Tuple<int, IX_PageHdr>(pageNum, pHdr);
        }

        public Tuple<RID, PF_PageHandle> GetNextFreeSlot(int size)
        {
            IsValid(-1);

            var tmp = GetNextFreePage(size);
            int pageNum = tmp.Item1;
            pHdr = (IX_PageHdr)tmp.Item2;

            PF_PageHandle ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);

            int slotNum = GetNumSlots(size);
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

        public void Open(IX_FileHdr<TK> hdr_tmp)
        {
            if (bFileOpen) throw new Exception();

            bFileOpen = true;

            hdr = hdr_tmp;

            if (hdr.extRecordSize <= 0) throw new Exception();

            bHdrChanged = true;

            IsValid(-1);
        }

        public IX_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            if (pHdr == null) pHdr = new IX_PageHdr();

            char[] buf = ph.pPageData;
            pHdr.From_buf(buf);
            return pHdr;
        }

        
        public void FlushPages()
        {
            if (!bFileOpen) throw new Exception();

            if (bHdrChanged)
            {
                // write to the filehdr
                IndexManagerUtil<TK>.WriteIndexFileHdr(hdr, ConverTKToString,pfHandle.fs, pfHandle.fd);
                bHdrChanged = false;
            }
            pfHandle.pf_bm.FlushPages(pfHandle.fd);
        }

        /// <summary>
        /// slot to set the record putting on the right page which have the same size for a single record 
        /// </summary>
        /// <param name="length"></param>
        /// <returns></returns>
        private int CalculateSize(int length,int flag)
        {
            // TODO defalut TK occupied 4 Bytes
            // Leaf:0
            if (flag == 0)
            {
                return (length - 3 * ConstProperty.Int_Size - 1) / ConstProperty.Int_Size;
            }
            else
            {
                return (length - 3 * ConstProperty.Int_Size-1) / (ConstProperty.RM_Page_RID_SIZE + ConstProperty.Int_Size);
            }
        }
    }
}
