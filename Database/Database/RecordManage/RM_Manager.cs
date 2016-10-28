using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.IndexManage.IndexValue;
using Database.Util;

namespace Database.RecordManage
{
    public class RM_Manager
    {
        PF_Manager pfm; // A reference to the external PF_Manager

        public RM_Manager(PF_Manager pfm)
        {
            this.pfm = pfm;
        }

        //
        // CreateFile
        //
        // Desc: Create a new RM table/file named fileName
        // with recordSize as the fixed size of records.
        // In:   fileName - name of file to create
        // In:   recordSize
        // Ret:  RM return code
        //
        public void CreateFile(string fileName, int recordSize,char[] data)
        {
            if ((recordSize >= ConstProperty.PF_PAGE_SIZE
                - ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap)
                || recordSize <0)
                throw new Exception();

            pfm.CreateFile(fileName);

            PF_FileHandle pfh = pfm.OpenFile(fileName);
            PF_PageHandle headerPage = pfh.AllocatePage();

            RM_FileHdr hdr = new RM_FileHdr();
            hdr.pf_fh.firstFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
            hdr.pf_fh.numPages = 1; // hdr page
            hdr.extRecordSize = recordSize;
            // For index
            if (data != null && data.Length != 0)
            {
                hdr.dataNum = data.Length;
                hdr.data = new char[hdr.dataNum];
                hdr.data = data;
            }

            // For index
            headerPage.pPageData = RecordUtil.SetFileHeaderToChar(hdr);

            pfh.MarkDirty(headerPage.pageNum);
            pfh.UnpinPage(headerPage.pageNum);

            pfm.CloseFile(pfh);
        }

        //
        // DestroyFile
        //
        // Desc: Delete a RM file named fileName (fileName must exist and not be open)
        // In:   fileName - name of file to delete
        // Ret:  RM return code
        //
        public void DestroyFile(string fileName)
        {
            pfm.DestroyFile(fileName);
        }

        //
        // OpenFile
        //
        // In:   fileName - name of file to open
        // Out:  fileHandle - refer to the open file
        //                    this function modifies local var's in fileHandle
        //       to point to the file data in the file table, and to point to the
        //       buffer manager object
        // Ret:  PF_FILEOPEN or other RM return code
        //
        public RM_FileHandle OpenFile(string fileName)
        {
            RM_FileHandle rmh = new RM_FileHandle();

            PF_FileHandle pfh = pfm.OpenFile(fileName);

            PF_PageHandle ph = pfh.GetThisPage(0);

            RM_FileHdr hdr = RecordUtil.GetFileHeader(ph);

            rmh.Open(pfh,hdr.extRecordSize);

            pfh.UnpinPage(0);

            return rmh;
        }

        //
        // CloseFile
        //
        // Desc: Close file associated with fileHandle
        //       The file should have been opened with OpenFile().
        // In:   fileHandle - handle of file to close
        // Out:  fileHandle - no longer refers to an open file
        //                    this function modifies local var's in fileHandle
        // Ret:  RM return code
        //
        public void CloseFile(RM_FileHandle rfileHandle)
        {
            if (!rfileHandle.bFileOpen || rfileHandle.pfHandle == null) throw new Exception();

            if (rfileHandle.bHdrChanged)
            {
                PF_PageHandle ph = rfileHandle.pfHandle.GetThisPage(0);
                rfileHandle.SetFileHeader(ph); // write hdr into file
                rfileHandle.pfHandle.MarkDirty(0);
                rfileHandle.pfHandle.UnpinPage(0);

                rfileHandle.ForcePages(ConstProperty.ALL_PAGES);
            }

            pfm.CloseFile(rfileHandle.pfHandle);
            rfileHandle.pfHandle = null;
            rfileHandle.bFileOpen = false;
        }
    }
}
