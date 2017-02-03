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
        PF_Manager pfm;
        public int treeHeight;

        private Func<TK, string> ConverTKToString;
        private Func<string, TK> ConverStringToTK;
        private Func<TK> CreatNewTK;
        private Func<TK, int> OccupiedNum;

        public IX_Manager(PF_Manager pfm, Func<TK, string> ConverTKToString, 
            Func<string, TK> ConverStringToTK, Func<TK> CreatNewTK, Func<TK, int> occupiedNum,int treeHeight =6)
        {
            this.treeHeight = treeHeight;
            this.pfm = pfm;
            this.ConverStringToTK = ConverStringToTK;
            this.ConverTKToString = ConverTKToString;
            this.CreatNewTK = CreatNewTK;
            this.OccupiedNum = occupiedNum;
        }

        public void CreateFile(string fileName, ConstProperty.AttrType indexType)
        {
            pfm.CreateFile(fileName);

            PF_FileHandle pfh = pfm.OpenFile(fileName);

            var hdr = CreatIndexFileHdr(pfh, IndexManagerUtil<int>.GetNodeDiskLength(), 30,treeHeight,1);
            char[] data = IndexManagerUtil<TK>.IndexFileHdrToCharArray(hdr, ConverTKToString);

            FileManagerUtil.WriteFileHdr(data, 0, pfm.fs);
            pfm.fs.Close();
            pfm.fs.Dispose();
        }

        public void DestroyFile(string fileName)
        {
            pfm.DestroyFile(fileName);
        }

        
        public IX_FileHandle<TK> OpenFile(string fileName,int treeDegree)
        {
            PF_FileHandle pfh = pfm.OpenFile(fileName);

            IX_FileHandle<TK> ixi = new IX_FileHandle<TK>(pfh, ConverTKToString, OccupiedNum);
            ixi.bFileOpen = true;

            var headerTmp = IndexManagerUtil<TK>.ReadIndexFileHdr(pfh,ConverStringToTK);

            var header = headerTmp as IX_FileHdr<TK>;

            if (header == null) throw new Exception();

            if (ixi == null) throw new Exception();

            IX_IndexHandle<TK> iih = new IX_IndexHandle<TK>(header.totalHeight,header.rootRID,ConverStringToTK,
                ConverTKToString,CreatNewTK,ixi, treeDegree);
            ixi.iih = iih;

            ixi.Open(header);

            return ixi;
        }

        public void CloseFile(IX_IndexHandle<TK> ixi)
        {

            if (!ixi.imp.bFileOpen || ixi.imp.pfHandle == null) throw new Exception();

            if (ixi.imp.bHdrChanged)
            {
                int num = IO.IOFDDic.FDMapping.Keys.Max() + 1;
                IndexManagerUtil<TK>.WriteIndexFileHdr(ixi.imp.hdr,ixi.FuncConverTKToString, ixi.imp.pfHandle.pf_bm.fs,num);

                ixi.imp.bHdrChanged = false;
            }

            if (!ixi.imp.pfHandle.bFileOpen) throw new IOException();
            ixi.imp.pfHandle.pf_bm.FlushPages(ixi.imp.pfHandle.fd);
            pfm.fs.Close();

            ixi.imp.pfHandle.bFileOpen = false;

            ixi.imp.pfHandle = null;
            ixi.imp.bFileOpen = false;
        }

        private IX_FileHdr<TK> CreatIndexFileHdr(PF_FileHandle pfh,int length,int recordSize,int totalHeight,int dicCount)
        {
            IX_FileHdr<TK> hdr = new IX_FileHdr<TK>();
            hdr.firstFree = pfh.hdr.firstFree;
            hdr.numPages = pfh.hdr.numPages;

            hdr.extRecordSize = recordSize;
            hdr.totalHeight = totalHeight;
            hdr.indexType = ConstProperty.AttrType.INT;


            hdr.rootRID = new RID(-1, -1);

            hdr.dicCount = dicCount;
            var dic = new Dictionary<int, int>();
            dic.Add(0, -1);
            dic.Add(1, -1);
            dic.Add(2, -1);
            dic.Add(3, -1);
            dic.Add(4, -1);
            dic.Add(5, -1);
            dic.Add(6, -1);
            dic.Add(-1, -1);
            dic.Add(-2, -1);
            dic.Add(-3, -1);
            dic.Add(-4, -1);
            dic.Add(-5, -1);
            dic.Add(-6, -1);
            hdr.dic = dic;
            return hdr;
        }
    }
}
