using Database;
using Database.Const;
using Database.IndexManage;
using Database.RecordManage;
using Database.SQLOperation;
using Database.SQLOperation.data;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.SystemManage
{
    public class SM_Manager<TK> where TK : IComparable<TK>
    {
        IX_Manager<TK> ixm;
        RM_Manager rmm;
        bool bDBOpen;
        RM_FileHandle relfh;
        RM_FileHandle attrfh;
        char[] cwd;
        Dictionary<string, string> parameters;

        private int TreeDegree { get; set; }

        public SM_Manager(IX_Manager<TK> ixm, RM_Manager rmm)
        {
            this.ixm = ixm;
            this.rmm = rmm;
        }

        public void OpenDb(string dbName)
        {
            if (dbName == null || dbName.Length ==0)
                throw new ArgumentNullException();
            if (bDBOpen)
            {
                throw new ArgumentNullException();
            }

            attrfh = rmm.OpenFile("attrcat");
            relfh = rmm.OpenFile("relcat");

            bDBOpen = true;
        }

        public void CloseDb()
        {
            if (!bDBOpen)
            {
                throw new ArgumentNullException();
            }
            rmm.CloseFile(attrfh);
            rmm.CloseFile(relfh);

            bDBOpen = false;
        }

        #region table
        public void CreateTable(
            string relName,
            int attrCount,
            IList<DataAttrInfo> attributes
            )
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0 || attrCount <= 0 || attributes == null)
            {
                throw new Exception();
            }

            if (relName.Equals("relcat") ||
               relName.Equals("attrcat"))
            {
                throw new Exception();
            }

            DataAttrInfo[] d = new DataAttrInfo[attrCount];
            int size = 0;
            for (int i = 0; i < attrCount; i++)
            {
                d[i] = new DataAttrInfo(attributes[i]);
                d[i].offset = size;
                size += attributes[i].attrLength;

                attrfh.InsertRec(DataAttrInfo.DataAttrInfoToChar(d[i]));
            }

            rmm.CreateFile(relName, size,"MetaData".ToArray());

            DataRelInfo rel = new DataRelInfo();
            rel.relName = relName.ToArray();
            rel.attrCount = attrCount;
            rel.recordSize = size;
            rel.numPages = 1; // initially
            rel.numRecords = 0;

            relfh.InsertRec(DataRelInfo.DataRelInfoToChar(rel));
        }

        // Get the first matching row for relName
        // contents are return in rel and the RID the record is located at is
        // returned in rid.
        // method returns SM_NOSUCHTABLE if relName was not found
        public Tuple<RID, DataRelInfo> GetRelFromCat(
            string relName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                relfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1, 
                // TODO
                0, 
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();

            if (rec == null)
                throw new NullReferenceException(); // no such table

            rfs.CloseScan();

            List<DataRelInfo> prel = new List<DataRelInfo>();
            var data = rec.GetData();
            var list = DataRelInfo.CharToDataRelInfo(data);
            if (list == null || list.Length == 0)
                throw new NullReferenceException();
            DataRelInfo rel = list.First();
            var rid = rec.GetRid();
            return new Tuple<RID, DataRelInfo>(rid, rel);
        }

        // Get the first matching row for relName, attrName
        // contents are returned in attr
        // location of record is returned in rid
        // method returns SM_NOSUCHENTRY if attrName was not found
        public Tuple<RID, DataAttrInfo> GetAttrFromCat(
            string relName,
            string attrName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();
            List<DataAttrInfo> infoList = 
                new List<DataAttrInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                infoList = DataAttrInfo.CharToDataAttrInfo(data).ToList();

                if (infoList != null && infoList.Count != 0)
                {
                    if (infoList.First().attrName.Equals(attrName))
                    {
                        break;
                    }
                }
            }
            rfs.CloseScan();
            if (infoList == null || infoList.Count == 0)
                throw new Exception();
            DataAttrInfo attr = new DataAttrInfo();
            attr = infoList.First();
            attr.func = Const.ConstProperty.AggFun.NO_F;
            var rid = rec.GetRid();
            return new Tuple<RID, DataAttrInfo>(rid, attr);
        }

        public void DropTable(string relName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName.Equals("relcat") ||
               relName.Equals("attrcat"))
            {
                throw new Exception();
            }

            RM_FileScan rfs = new RM_FileScan();

            rfs.OpenScan(
                relfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();
            List<DataRelInfo> dataRelInfoList = new List<DataRelInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                dataRelInfoList = DataRelInfo.CharToDataRelInfo(data).ToList();

                if (dataRelInfoList != null && dataRelInfoList.Count != 0)
                {
                    if (dataRelInfoList.First().relName.Equals(relName))
                    {
                        break;
                    }
                }
            }
            rfs.CloseScan();

            if (dataRelInfoList == null || dataRelInfoList.Count == 0)
                throw new Exception();
            var rid = rec.GetRid();
            rmm.DestroyFile(relName);
            relfh.DeleteRec(rid);

            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            rec = rfs.GetNextRec();
            List<DataAttrInfo> infoList =
                new List<DataAttrInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                infoList = DataAttrInfo.CharToDataAttrInfo(data).ToList();

                if (infoList != null && infoList.Count != 0)
                {
                    if (infoList.First().relName.Equals(relName))
                    {
                        rid = rec.GetRid();
                        attrfh.DeleteRec(rid);
                        DropIndex(relName, new string(infoList[0].attrName));
                    }
                }
            }
            rfs.CloseScan();
        }
        #endregion

        #region index
        public void CreateIndex(
            string relName,
            string attrName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
            {
                throw new Exception();
            }

            var tuple = GetAttrFromCat(relName, attrName);
            DataAttrInfo attr = tuple.Item2;
            RID rid = tuple.Item1;

            // index already exists
            if (attr.indexNo != -1)
                throw new Exception();

            attr.indexNo = attr.offset;
            // TODO
            ixm.CreateFile(relName, ConstProperty.AttrType.INT);

            // update attrcat
            // TODO

            // now create index entries
            var ixh = ixm.OpenFile(relName, TreeDegree);
            RM_FileHandle rfh = rmm.OpenFile(relName);

            var tuple2 = GetFromTable(relName);
            var attrCount = tuple2.Item1;
            var attributes = tuple2.Item2;

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                rfh,
                attr.attrType,
                attr.attrLength,
                attr.offset,
                ConstProperty.CompOp.NO_OP,
                null,
                ConstProperty.ClientHint.NO_HINT
                );

            RM_Record rec = rfs.GetNextRec(); 
            while (rec != null)
            {
                var pdata = rec.GetData();
                rid = rec.GetRid();
                // TODO
                //ixh.InsertEntry();
            }

            rfs.CloseScan();
            rfh.IsValid(-1);
            ixm.CloseFile(ixh.iih);
        }

        public void DropIndex(
            string relName,
            string attrName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0 || attrName == null || attrName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();
            List<DataAttrInfo> infoList =
                new List<DataAttrInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                infoList = DataAttrInfo.CharToDataAttrInfo(data).ToList();

                if (infoList != null && infoList.Count != 0)
                {
                    if (infoList.First().attrName.Equals(attrName))
                    {
                        infoList[0].indexNo = -1;
                        break;
                    }
                }
            }
            rfs.CloseScan();
            if (infoList == null || infoList.Count == 0)
                throw new Exception();
            var rid = rec.GetRid();
            ixm.DestroyFile(relName);

            // update attrcat
            // TODO
        }

        public void DropIndexFromAttrCatAlone(
            string relName,
            string attrName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();
            List<DataAttrInfo> infoList =
                new List<DataAttrInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                infoList = DataAttrInfo.CharToDataAttrInfo(data).ToList();

                if (infoList != null && infoList.Count != 0)
                {
                    if (infoList.First().attrName.Equals(attrName))
                    {
                        infoList[0].indexNo = -1;
                        break;
                    }
                }
            }
            rfs.CloseScan();

            // update attrcat
            // TODO
        }

        public void ResetIndexFromAttrCatAlone(
            string relName,
            string attrName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();
            List<DataAttrInfo> infoList =
                new List<DataAttrInfo>();
            while (rec != null)
            {
                var data = rec.GetData();
                infoList = DataAttrInfo.CharToDataAttrInfo(data).ToList();

                if (infoList != null && infoList.Count != 0)
                {
                    if (infoList.First().attrName.Equals(attrName))
                    {
                        infoList[0].indexNo = infoList[0].offset;
                        break;
                    }
                }
            }
            rfs.CloseScan();
            // update attrcat
            // TODO
        }
        #endregion

        #region Load record
        public void LoadRecord(string relName,
                          int buflen,
                          string buf)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
            {
                throw new Exception();
            }

            RM_FileHandle rfh = rmm.OpenFile(relName);
            int attrCount = -1;
            var tuple = GetFromTable(relName);
            attrCount = tuple.Item1;
            List<DataAttrInfo> attributes = new List<DataAttrInfo>();
            attributes = tuple.Item2.ToList();

            var indexes = new IX_FileHandle<TK>[attrCount];

            int size = 0;
            for (int i = 0; i < attrCount; i++)
            {
                size += attributes[i].attrLength;
                if (attributes[i].indexNo != -1)
                {
                    indexes[i] = ixm.OpenFile(relName,TreeDegree);
                }
            }

            if (size != buflen) throw new NullReferenceException();
            var rid = rfh.InsertRec(buf.ToArray());

            // TODO
            //for (int i = 0; i < attrCount; i++)
            //{
            //    if (attributes[i].indexNo != -1)
            //    {
            //        // cerr << "SM loadRecord index - inserting {" << *(char*)(buf +
            //        // attributes[i].offset) << "} " << rid << endl;
            //        char* ptr = const_cast<char*>(buf + attributes[i].offset);
            //        rc = indexes[i].InsertEntry(ptr,
            //                                    rid);
            //        if (rc != 0) return rc;
            //    }
            //}

            var tuple2 = GetRelFromCat(relName);
            var r = tuple2.Item2;
            rid = tuple2.Item1;

            r.numRecords += 1;
            r.numPages = rfh.GetNumPages();
            // TODO
            RM_Record rec = new RM_Record();
            relfh.UpdateRec(rec);
            rmm.CloseFile(rfh);

            for (int i = 0; i < attrCount; i++)
            {
                if (attributes[i].indexNo != -1)
                {
                    ixm.CloseFile(indexes[i].iih);
                }
            }
        }

        public void Load(
            string relName,
            string attrName)
        {
            throw new NotImplementedException();
        }
        #endregion

        public bool IsValid()
        {
            bool ret = true;
            ret = ret && bDBOpen;
            return ret ? true :false;
        }

        public Tuple<int, DataAttrInfo[]> GetFromTable(string relName)
        {
            var invalid = IsValid();
            if (invalid) throw new Exception();

            if (relName == null || relName.Length != 0)
                throw new Exception();

            RM_FileScan rfs = new RM_FileScan();
            rfs.OpenScan(
                relfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            var rec = rfs.GetNextRec();

            if (rec == null)
                throw new NullReferenceException(); // no such table

            rfs.CloseScan();

            List<DataRelInfo> prel = new List<DataRelInfo>();
            var data = rec.GetData();
            var list = DataRelInfo.CharToDataRelInfo(data);
            if (list == null || list.Length == 0)
                throw new NullReferenceException();
            DataRelInfo rel = list.First();
            var rid = rec.GetRid();

            var attrCount = rel.attrCount;
            var attributes = new DataAttrInfo[attrCount];

            rfs.OpenScan(
                attrfh,
                ConstProperty.AttrType.STRING,
                ConstProperty.MAXSTRINGLEN + 1,
                // TODO
                0,
                ConstProperty.CompOp.EQ_OP,
                relName,
                ConstProperty.ClientHint.NO_HINT
                );

            int numRecs = 0;
            rec = rfs.GetNextRec();
            while (true)
            {
                if (rec == null || numRecs > attrCount)
                    break;
                List<DataAttrInfo> pattr = new List<DataAttrInfo>();
                data = rec.GetData();
                attributes[numRecs] = DataAttrInfo.CharToDataAttrInfo(data).First();
                numRecs++;
            }
            rfs.CloseScan();
            if (attrCount != attributes.Length)
            {
                throw new Exception();
            }
            return new Tuple<int, DataAttrInfo[]>(attrCount, attributes);
        }

        // ra.relName must be NULL when we start off and should be free()d by the user
        public void FindRelForAttr(RelAttr ra, int nRelations, string[] possibleRelations)
        {
            IsValid();
            if (ra.relName != null) return;
            bool found = false;
            for (int i = 0; i < nRelations; i++)
            {
               var tuple = GetAttrFromCat(possibleRelations[i], new string(ra.attrName));
                DataAttrInfo a = tuple.Item2;
                RID rid=tuple.Item1;

                if (!found)
                {
                    found = true;
                    ra.relName = possibleRelations[i].ToArray();
                    break;
                }

                if (found == false) throw new Exception();
            }
        }

        public int GetNumPages(string relName)
        {
            var tuple = GetRelFromCat(relName);
            DataRelInfo r = tuple.Item2;
            return r.numPages;
        }

        public int GetNumRecords(string relName)
        {
            var tuple = GetRelFromCat(relName);
            DataRelInfo r = tuple.Item2;
            return r.numRecords;
        }
    }
}
