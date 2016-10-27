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
        public static RM_FileHdr GetFileHeader(PF_PageHandle ph)
        {
            RM_FileHdr hdr = new RM_FileHdr();
            string str = new string(ph.pPageData);
            Int32.TryParse(str.Substring
                (0, ConstProperty.PF_PageHdr_SIZE)
                , out hdr.pf_fh.firstFree);
            Int32.TryParse(str.Substring
                (ConstProperty.PF_PageHdr_SIZE
                , 2 * ConstProperty.PF_PageHdr_SIZE)
                , out hdr.pf_fh.numPages);
            Int32.TryParse(str.Substring
                (2 * ConstProperty.PF_PageHdr_SIZE
                , 3 * ConstProperty.PF_PageHdr_SIZE), out hdr.extRecordSize);

            return hdr;
        }

        // TODO
        public static IX_FileHdr GetIndexHeader(PF_PageHandle ph)
        {
            return default(IX_FileHdr);
        }

        // For index=> convert to IX_FileHdr
        // TODO
        public static char[] SetFileHeaderToChar(RM_FileHdr hdr)
        {
            char[] content = new char[3 * ConstProperty.PF_PageHdr_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.pf_fh.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.pf_fh.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            return content;
        }
    }
}
