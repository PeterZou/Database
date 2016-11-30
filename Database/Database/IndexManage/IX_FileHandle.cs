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

        public IX_FileHandle(IX_IndexHandle<TK> iih)
        {
            bFileOpen = false;
            bHdrChanged = false;
            this.iih = iih;
        }

        override public void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = IndexManagerUtil<TK>.WriteIndexFileHdr(hdr, iih.ConverTKToString);
        }

        override public int fullRecordSize(int size)
        {
            // TODO defalut TK occupied 4 Bytes
            return 4 *ConstProperty.Int_Size+size* ConstProperty.Int_Size + size*ConstProperty.RM_Page_RID_SIZE;
        }

        override public void DeleteRec(RID rid)
        {
            throw new NotImplementedException();
        }

        override public RID InsertRec(char[] pData)
        {
            throw new NotImplementedException();
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

        public void Open(PF_FileHandle pfh, IX_FileHdr<TK> hdr_tmp)
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

        public Node<TK, RIDKey<TK>> ShowBplustree(RID rid)
        {
            return null;
        }

        public IX_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            if (pHdr == null) pHdr = new IX_PageHdr();

            char[] buf = ph.pPageData;
            pHdr.From_buf(buf);
            return pHdr;
        }

        private int CalculateSize(int length)
        {
            // TODO defalut TK occupied 4 Bytes
            return (length - 4 * ConstProperty.Int_Size) / (ConstProperty.RM_Page_RID_SIZE + ConstProperty.Int_Size);
        }
    }
}
