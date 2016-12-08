using Database.Const;
using Database.FileManage;
using Database.IndexManage.IndexValue;
using Database.RecordManage;
using System;
using System.Collections.Generic;
using System.IO;
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
            // set header
            NodeDisk<TK> nd = new NodeDisk<TK>();

            rmm.CreateFile(fileName, recordSize, new char[] { '1' ,'2'});
        }

        public void DestroyFile(string fileName)
        {
            rmm.DestroyFile(fileName);
        }

        
        public IX_FileHandle<TK> OpenFile(string fileName)
        {
            IX_IndexHandle<TK> iih = new IX_IndexHandle<TK>(
                );

            IX_FileHandle<TK> ixi = new IX_FileHandle<TK>(iih);

            var headerTmp = IndexManagerUtil<TK>.ReadIndexFileHdr(rmm.pfm.fs,ixi.iih.ConverStringToTK);

            var header = headerTmp as IX_FileHdr<TK>;

            if (header == null) throw new Exception();

            if (ixi == null) throw new Exception();
            ixi.Open(ixi.pfHandle, header);

            return ixi;
        }

        public void CloseFile(IX_IndexHandle<TK> ixi)
        {

            if (!ixi.imp.bFileOpen || ixi.imp.pfHandle == null) throw new Exception();

            if (ixi.imp.bHdrChanged)
            {
                int num = IO.IOFDDic.FDMapping.Keys.Max() + 1;
                IndexManagerUtil<TK>.WriteIndexFileHdr(ixi.imp.hdr,ixi.ConverTKToString);

                ixi.imp.bHdrChanged = false;
            }

            if (!ixi.imp.pfHandle.bFileOpen) throw new IOException();
            ixi.imp.pfHandle.pf_bm.FlushPages(ixi.imp.pfHandle.fd);
            rmm.pfm.fs.Close();

            ixi.imp.pfHandle.bFileOpen = false;

            ixi.imp.pfHandle = null;
            ixi.imp.bFileOpen = false;
        }
    }
}
