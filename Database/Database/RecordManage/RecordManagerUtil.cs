using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.FileManage;
using Database.IndexManage;

namespace Database.RecordManage
{
    public static class RecordManagerUtil
    {
        // For index=> convert to IX_FileHdr
        public static char[] SetFileHeaderToChar(RM_FileHdr hdr)
        {
            char[] content = new char[ConstProperty.PF_FILE_HDR_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            if (hdr.data != null)
            {
                FileManagerUtil.ReplaceTheNextFree(content, hdr.data, 3 * ConstProperty.PF_PageHdr_SIZE, hdr.data.Length);
            }

            return content;
        }
    }
}
