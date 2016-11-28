using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.FileManage;
using Database.Interface;
using Database.RecordManage;
using Database.Const;

namespace Database.IndexManage
{
    // 11282016 抽象FileHandle主要是因为Index保存的主要是非固定数据
    // 非固定情况下，page的选择策略：
    // 主要问题：1.由于可以删除，这时候插入与删除大小不一致的数据时，1）插不进去2）插进去，但是会很多剩余的间隙
    // 策略：
    //       1）bitMap,预先设置一个定值，随着插入和删除动态的增加和删减
    //       2）data,尽可能在一页插入数据，1)遍历后发现所有的slot都比较小，做gc,压缩
    //                                   2)压缩之后发现还是不够插入，①保留压缩后的data为以后插入做准备，②循环进入下一页重复
    public class IX_FileHandle<TK>: FileHandle
    {
        public IX_FileHdr<TK> hdr;// file header
        public IX_PageHdr pHdr;

        #region Unique
        public IX_FileHandle()
        {
            bFileOpen = false;
            bHdrChanged = false;
        }

        public void Open(PF_FileHandle pfh, IX_FileHdr<TK> hdr_tmp)
        { }

        public Tuple<RID, PF_PageHandle> GetNextFreeSlot(int recordSize)
        {
            IsValid();

            var tmp = GetNextFreePage(recordSize);
            int pageNum = tmp.Item1;
            pHdr = tmp.Item2;

            PF_PageHandle ph = pfHandle.GetThisPage(pageNum);
            pfHandle.UnpinPage(pageNum);

            int slotNum = GetNumSlots(pageNum);
            var bitmap = new RecordManage.Bitmap(pHdr.freeSlotMap, slotNum);

            for (UInt32 i = 0; i < slotNum; i++)
            {
                if (bitmap.Test(i))
                {
                    return new Tuple<RID, PF_PageHandle>(new RID(pageNum, (int)i), ph);
                }
            }

            throw new Exception();
        }

        /// <summary>
        /// 先查找有没有比maxSize的以前删除的记录，有的话直接插入，没有的话，压缩
        /// </summary>
        /// <returns></returns>
        public Tuple<int, IX_PageHdr> GetNextFreePage(int recordSize)
        {
            // while循环直到找到能够放置这个record的页面


            return null;
        }

        public IX_PageHdr GetPageHeader(PF_PageHandle ph)
        {
            return null;
        }

        public void SetPageHeader(PF_PageHandle ph, IX_PageHdr pHdr)
        { }
        #endregion

        /// <summary>
        /// 在使用GetNextFreePage后，重新获取该pageNum下的slotNum
        /// </summary>
        /// <returns></returns>
        public int GetNumSlots(int pageNum)
        {
            return 0;
        }

        public override int GetNumPages()
        {
            throw new NotImplementedException();
        }

        public override RM_Record GetRec(RID rid)
        {
            throw new NotImplementedException();
        }

        public override RID InsertRec(char[] pData)
        {
            throw new NotImplementedException();
        }

        public override void DeleteRec(RID rid)
        {
            throw new NotImplementedException();
        }

        public override void UpdateRec(RM_Record rec)
        {
            throw new NotImplementedException();
        }

        public override void IsValid()
        {
            throw new NotImplementedException();
        }

        public override void SetFileHeader(PF_PageHandle ph)
        {
            throw new NotImplementedException();
        }  
    }
}
