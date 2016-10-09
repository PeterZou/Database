using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.BufferManage;
using Database.Const;
using System.Runtime.InteropServices;
using System.IO;

namespace Database.FileManage
{
    public class PF_Manager
    {
        private PF_Buffermgr pBufferMgr;

        public PF_Manager()
        {
            pBufferMgr = new PF_Buffermgr(ConstProperty.PF_BUFFER_SIZE);
        }

        //
        // CreateFile
        //
        // Desc: Create a new PF file named fileName
        // In:   fileName - name of file to create
        // Ret:  PF return code
        //
        public void CreateFile(string fileName)
        {
            if (File.Exists(fileName))
            {
                File.Delete(fileName);
            }

            using (FileStream myFs = new FileStream(fileName, FileMode.Create))
            {
                using (StreamWriter mySw = new StreamWriter(myFs))
                {
                    PF_FileHdr hdr;
                    hdr.firstFree = (int)ConstProperty.Page_statics.PF_PAGE_LIST_END;
                    hdr.numPages = 0;

                    int num = IO.IOFDDic.FDMapping.Keys.Max();
                    IO.IOFDDic.FDMapping.Add(num+1, fileName);

                    FileManagerUtil.WriteFileHdr(hdr, num + 1);
                    mySw.Write(hdr);
                }
            }
        }

        //
        // DestroyFile
        //
        // Desc: Delete a PF file named fileName (fileName must exist and not be open)
        // In:   fileName - name of file to delete
        // Ret:  PF return code
        //
        public void DestroyFile(string fileName)
        {
            try
            {
                File.Delete(fileName);
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
        }

        //
        // OpenFile
        //
        // Desc: Open the paged file whose name is "fileName".  It is possible to open
        //       a file more than once, however, it will be treated as 2 separate files
        //       (different file descriptors; different buffers).  Thus, opening a file
        //       more than once for writing may corrupt the file, and can, in certain
        //       circumstances, crash the PF layer. Note that even if only one instance
        //       of a file is for writing, problems may occur because some writes may
        //       not be seen by a reader of another instance of the file.
        // In:   fileName - name of file to open
        // Out:  fileHandle - refer to the open file
        //                    this function modifies local var's in fileHandle
        //       to point to the file data in the file table, and to point to the
        //       buffer manager object
        // Ret:  PF_FILEOPEN or other PF return code
        //
        public PF_FileHandle OpenFile(string fileName)
        {
            try
            {
                PF_FileHdr hdr = FileManagerUtil.ReadFileHdr(fileName);

                PF_Buffermgr pf_bm = new PF_Buffermgr(ConstProperty.PF_BUFFER_SIZE);
                PF_FileHandle pf_fh = new PF_FileHandle(hdr, fileName, pf_bm, true);
                return pf_fh;
            }
            catch (IOException e)
            {
                throw new IOException(e.ToString());
            }
            
        }

        public void CloseFile(PF_FileHandle pf_fh)
        {
            if (!pf_fh.bFileOpen) throw new IOException();
            pf_fh.FlushPages();

            pf_fh.bFileOpen = false;
        }
    }
}
