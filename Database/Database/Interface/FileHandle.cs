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

        abstract public int GetNumPages();

        abstract public void IsValid();

        abstract public RM_Record GetRec(RID rid);

        abstract public RID InsertRec(char[] pData);

        abstract public void UpdateRec(RM_Record rec);

        abstract public void DeleteRec(RID rid);

        abstract public void SetFileHeader(PF_PageHandle ph);
    }
}
