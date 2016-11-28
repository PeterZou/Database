using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.FileManage;
using Database.RecordManage;
using Database.Const;
using Database.Interface;

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
    public class IX_FileHandle<TK> : FileHandle
        where TK : IComparable<TK>
    {
        public IX_IndexHandle<TK> iih;

        public IX_FileHdr<TK> hdr;                        // file header

        public IX_FileHandle(IX_IndexHandle<TK> iih)
        {
            bFileOpen = false;
            bHdrChanged = false;
            this.iih = iih;
        }

        public override int fullRecordSize() { return hdr.extRecordSize; }

        public override void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = IndexManagerUtil<TK>.WriteIndexFileHdr(hdr, iih.ConverTKToString);
        }

        // TODO
        public override Tuple<int, RM_PageHdr> GetNextFreePage()
        {
            int pageNum = -3;
            IsValid();
            PF_PageHandle ph;
            // TODO
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

        public override void DeleteRec(RID rid)
        {
            throw new NotImplementedException();
        }

        public override RID InsertRec(char[] pData)
        {
            throw new NotImplementedException();
        }

        public override void UpdateRec(RM_Record rec)
        {
            throw new NotImplementedException();
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

            IsValid();
        }

        public RM_Record GetRec(RID rid)
        {
            return null;
        }
    }
}
