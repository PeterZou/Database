using Database.FileManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Database.Const;

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
        public void CreateFile(string fileName, int recordSize)
        {
            if ((recordSize >= ConstProperty.PF_PAGE_SIZE
                - ConstProperty.RM_Page_Hdr_SIZE_ExceptBitMap)
                || recordSize <0)
                throw new Exception();

            pfm.CreateFile(fileName);

            // TODO
        }
    }
}
