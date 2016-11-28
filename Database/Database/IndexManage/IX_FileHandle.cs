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
    {
        public IX_FileHdr<TK> hdr;                        // file header

        public IX_FileHandle()
        {
            bFileOpen = false;
            bHdrChanged = false;
        }

        override public int fullRecordSize() { return hdr.extRecordSize; }

        public override void SetFileHeader(PF_PageHandle ph)
        {
            ph.pPageData = IndexManagerUtil<TK>.SetIndexHeaderToChar(hdr);
        }

        public override Tuple<int, RM_PageHdr> GetNextFreePage()
        {
            throw new NotImplementedException();
        }

        public override int GetNumPages()
        {
            throw new NotImplementedException();
        }

        public override void DeleteRec(RID rid)
        {
            throw new NotImplementedException();
        }

        public override RID InsertRec(char[] pData)
        {
            throw new NotImplementedException();
        }

        public override void Open(PF_FileHandle pfh, RM_FileHdr hdr_tmp)
        {
            throw new NotImplementedException();
        }
    }
}
