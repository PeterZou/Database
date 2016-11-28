using Database.Const;
using Database.FileManage;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.IndexManage
{
    public class IX_Manager<TK>
        where TK : IComparable<TK>
    {
        RM_Manager rmm;

        public IX_Manager(RM_Manager rmm)
        {
            this.rmm = rmm;
        }

        public void CreateFile(string fileName, int recordSize, ConstProperty.AttrType indexType)
        {
            NodeDisk<TK> nd = new NodeDisk<TK>();

            rmm.CreateFile(fileName, recordSize, new char[] { '1' ,'2'});
        }

        public void DestroyFile(string fileName)
        {
            rmm.DestroyFile(fileName);
        }

        
        public IX_IndexHandle<TK> OpenFile(string fileName)
        {
            RM_FileHandle rmf = rmm.OpenFile(fileName);

            IX_IOHandle<TK> iX_IOHandle = new IX_IOHandle<TK>(rmf, null, null);

            IX_FileHdr<TK> header = new IX_FileHdr<TK>();

            IX_IndexHandle<TK> ixi = new IX_IndexHandle<TK>(iX_IOHandle, header.totalHeight, new RID(1,1));

            return ixi;
        }

        public void CloseFile(IX_IndexHandle<TK> ixi)
        {
            rmm.CloseFile(ixi.iX_IOHandle.rmp);
        }
    }
}
