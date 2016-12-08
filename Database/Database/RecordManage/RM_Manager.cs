using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;
using Database.IndexManage.IndexValue;
using System.IO;

namespace Database.RecordManage
{
    public class RM_Manager
    {
        public PF_Manager pfm; // A reference to the external PF_Manager

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

            RM_FileHdr rmf = new RM_FileHdr();
            rmf.firstFree = pfh.hdr.firstFree;
            rmf.numPages = pfh.hdr.numPages;
            rmf.extRecordSize = recordSize;
            rmf.data = data;

            int num = IO.IOFDDic.FDMapping.Keys.Max() + 1;

            FileManagerUtil.WriteRecordHdr(rmf, num, pfm.fs);
            pfm.fs.Close();
            pfm.fs.Dispose();
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
            var hdr = FileManagerUtil.ReadFileHdr(fileName, pfm.fs, ConstProperty.FileType.Record);
            var hdrTmp = hdr as RM_FileHdr;
            if (hdrTmp == null) throw new Exception();
            rmh.Open(pfh, hdrTmp);

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
                int num = IO.IOFDDic.FDMapping.Keys.Max() + 1;
                FileManagerUtil.WriteRecordHdr(rfileHandle.hdr, num, pfm.fs);

                rfileHandle.bHdrChanged = false;
            }

            if (!rfileHandle.pfHandle.bFileOpen) throw new IOException();
            rfileHandle.pfHandle.pf_bm.FlushPages(rfileHandle.pfHandle.fd);
            pfm.fs.Close();

            rfileHandle.pfHandle.bFileOpen = false;

            rfileHandle.pfHandle = null;
            rfileHandle.bFileOpen = false;
        }
    }
}
