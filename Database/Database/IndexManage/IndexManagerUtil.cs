using Database.Const;
using Database.FileManage;
using Database.IndexManage.BPlusTree;
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
    public static class IndexManagerUtil<TK>
        where TK : IComparable<TK>
    {
        public static char[] IndexFileHdrToCharArray(IX_FileHdr<TK> hdr, Func<TK, string> ConverTKToString)
        {
            char[] content = new char[ConstProperty.PF_FILE_HDR_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
               , hdr.totalHeight, 3 * ConstProperty.PF_PageHdr_SIZE);

            // IndexType
            FileManagerUtil.ReplaceTheNextFree(content
                , (int)hdr.indexType, 4 * ConstProperty.PF_PageHdr_SIZE, 1);

            // RootRID
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.rootRID.Page, 4 * ConstProperty.PF_PageHdr_SIZE + 1, 4);
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.rootRID.Slot, 4 * ConstProperty.PF_PageHdr_SIZE + 5, 4);

            // dicCount
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.dic.Keys.Count, 4 * ConstProperty.PF_PageHdr_SIZE + 9, ConstProperty.PF_PageHdr_SIZE);

            // KandV
            char[] charArray2 = SetDicToChar(hdr.dic);
            FileManagerUtil.ReplaceTheNextFree(content
                , charArray2, 5 * ConstProperty.PF_PageHdr_SIZE + 9, charArray2.Length);

            return content;
        }

        public static void WriteIndexFileHdr(IX_FileHdr<TK> hdr, Func<TK, string> ConverTKToString,
            FileStream fs, int fd)
        {
            var data = IndexFileHdrToCharArray(hdr, ConverTKToString);
            FileManagerUtil.WriteFileHdr(data,fd,fs);
        }

        public static interfaceFileHdr ReadIndexFileHdr(PF_FileHandle pfh, Func<string, TK> ConverStringToTK)
        {
            var fs = pfh.fs;
            return ReadIndexFileHdr(ConverStringToTK, fs);
        }

        public static interfaceFileHdr ReadIndexFileHdr(string filePath, Func<string, TK> ConverStringToTK)
        {
            interfaceFileHdr obj = null;
            using (FileStream fs = new FileStream(filePath, FileMode.Open))
            {
                obj = ReadIndexFileHdr(ConverStringToTK, fs);
            }
            return obj;
        }

        private static interfaceFileHdr ReadIndexFileHdr(Func<string, TK> ConverStringToTK, FileStream fs)
        {
            try
            {
                fs.Position = 0;
                StreamReader sr = new StreamReader(fs);

                var pf_fh = new IX_FileHdr<TK>();
                FileManagerUtil.ExtractFile(sr, pf_fh);
                ExtractIndex(sr, pf_fh, ConverStringToTK);
                return pf_fh;
            }
            catch (IOException e)
            {
                fs.Close();
                throw new IOException(e.ToString());
            }
        }

        public static char[] SetNodeDiskToChar(NodeDisk<TK> nl, Func<TK, string> ConverTKToString)
        {
            char[] data = new char[nl.length];
            // length
            FileManagerUtil.ReplaceTheNextFree(data, nl.length, 0);
            // isLeaf
            data[ConstProperty.Int_Size] = Convert.ToChar(nl.isLeaf+48);
            // capacity
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, ConstProperty.Int_Size + 1);
            // height
            FileManagerUtil.ReplaceTheNextFree(data, nl.height, 2 * ConstProperty.Int_Size + 1);

            // keyList
            for (int i = 0; i < nl.capacity; i++)
            {
                char[] str = ConverTKToString(nl.keyList[i]).ToArray();
                FileManagerUtil.ReplaceTheNextFree(data, str,
                    (3 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size);
            }

            // childRidList
            // TODO
            if (nl.childRidList != null && nl.childRidList.Count != 0)
            {
                for (int i = 0; i < nl.capacity + 1; i++)
                {
                    FileManagerUtil.ReplaceTheNextFree(data, nl.childRidList[i].Page,
                        (3 + nl.capacity + 2 * i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size);

                    FileManagerUtil.ReplaceTheNextFree(data, nl.childRidList[i].Slot,
                        (3 + nl.capacity + 2 * i) * ConstProperty.Int_Size + 1 + ConstProperty.Int_Size, ConstProperty.Int_Size);
                }
            }
            
            return data;
        }

        public static NodeDisk<TK> SetCharToNodeDisk(char[] data, int length, Func<string, TK> ConverStringToTK)
        {
            string tmpStr = new string(data);
            string dataStr = tmpStr.Substring(ConstProperty.Int_Size, length- ConstProperty.Int_Size);
            NodeDisk<TK> node = new NodeDisk<TK>();
            node.length = length;
            node.isLeaf = Convert.ToInt32(dataStr.Substring(0, 1));
            node.capacity = Convert.ToInt32(dataStr.Substring(1, ConstProperty.Int_Size));
            node.height = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size + 1, ConstProperty.Int_Size));

            node.keyList = new List<TK>();
            for (int i = 0; i < node.capacity; i++)
            {
                TK tmp = ConverStringToTK(dataStr.Substring((2 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                node.keyList.Add(tmp);
            }

            node.childRidList = new List<RID>();
            if (node.capacity != 0 && node.isLeaf == 1)
            {
                for (int i = 0; i < node.capacity + 1; i++)
                {
                    int num = (2 + node.capacity + 2*i) * ConstProperty.Int_Size + 1;
                    int pageNum = Convert.ToInt32(dataStr.Substring(
                        num, ConstProperty.Int_Size));
                    int slotNum = Convert.ToInt32(dataStr.Substring(
                        num+ ConstProperty.Int_Size, ConstProperty.Int_Size));
                    node.childRidList.Add(new RID(pageNum, slotNum));
                }
            }
            
            return node;
        }

        public static int GetNodeDiskLength()
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            return length;
        }

        public static int GetNodeDiskLengthWithChild(int size,int slotNum)
        {
            int num = ConstProperty.RM_Page_RID_SIZE + ConstProperty.Int_Size;
            return (GetNodeDiskLength() + (size - 1) * (num) + ConstProperty.RM_Page_RID_SIZE) * slotNum;
        }

        public static int GetNodeDiskLength(NodeDisk<TK> nl)
        {
            int length = 0;

            length += 3 * ConstProperty.Int_Size + 1;

            // TODO Set TK is int
            length += nl.capacity * ConstProperty.Int_Size;
            if(nl.childRidList != null)length += nl.capacity * ConstProperty.RM_Page_RID_SIZE + ConstProperty.RM_Page_RID_SIZE;
            return length;
        }

        private static void ExtractIndex(StreamReader sr, IX_FileHdr<TK> pf_fh, Func<string, TK> ConverStringToTK)
        {
            sr.BaseStream.Seek(0, SeekOrigin.Begin);

            char[] extRecordSize = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] totalHeight = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] indexType = new char[1];
            char[] lengthChar = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            char[] dicCount = new char[ConstProperty.PF_FILE_HDR_NumPages_SIZE];
            int length = 0;

            sr.Read(extRecordSize, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(extRecordSize), out pf_fh.extRecordSize);

            sr.Read(totalHeight, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(totalHeight), out pf_fh.totalHeight);

            int num = -1;
            sr.Read(indexType, 0, 1);
            Int32.TryParse(new string(indexType), out num);
            pf_fh.indexType = (ConstProperty.AttrType)num;

            // NodeDisk<TK>
            // 读入NodeDisk<TK>的length确定data[]的长度
            int pageNum = 0;
            int slotNum = 0;
            sr.Read(lengthChar, 0, ConstProperty.Int_Size);
            Int32.TryParse(new string(lengthChar), out pageNum);
            sr.Read(lengthChar, 0, ConstProperty.Int_Size);
            Int32.TryParse(new string(lengthChar), out slotNum);

            pf_fh.rootRID = new RID(pageNum, slotNum);

            // DicCount
            sr.Read(dicCount, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(dicCount), out pf_fh.dicCount);

            int sum = ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue;
            char[] data2 = new char[pf_fh.dicCount* sum];
            sr.Read(data2, 0, pf_fh.dicCount * sum);
            pf_fh.dic = SetCharToDic(data2, pf_fh.dicCount * sum);
        }

        private static char[] SetDicToChar(Dictionary<int, int> dic)
        {
            List<char> charList = new List<char>();

            foreach (var d in dic)
            {
                char[] array = new char[ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue];
                FileManagerUtil.ReplaceTheNextFree(array, d.Key, 0, ConstProperty.IndexHeaderKey);
                FileManagerUtil.ReplaceTheNextFree(array, d.Value, ConstProperty.IndexHeaderKey, ConstProperty.IndexHeaderValue);
                charList.AddRange(array);
            }

            return charList.ToArray();
        }

        private static Dictionary<int,int> SetCharToDic(char[] data, int length)
        {
            Dictionary<int, int> dic = new Dictionary<int, int>();

            int sum = ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue;
            string str = new string(data);
            for (int i = 0; i < length / sum; i++)
            {
                int numKey = 0;
                int numValue = 0;
                string strKey = str.Substring(sum*i, ConstProperty.IndexHeaderKey);
                string strValue = str.Substring(sum * i+ ConstProperty.IndexHeaderKey, ConstProperty.IndexHeaderValue);
                Int32.TryParse(strKey, out numKey);
                Int32.TryParse(strValue, out numValue);
                dic.Add(numKey, numValue);
            }
            return dic;
        }

        public static Node<TK, RIDKey<TK>> ConvertNodeDiskToNode(NodeDisk<TK> nodeDisk,
            RID rid, Func<TK> creatNewTK, List<RID> ridlist)
        {
            Node<TK, RIDKey<TK>> node = new Node<TK, RIDKey<TK>>();
            node.CurrentRID = new RIDKey<TK>(rid, creatNewTK());
            node.Height = nodeDisk.height;
            node.Values = new List<TK>();
            node.Property = new List<RIDKey<TK>>();

            // leaf:0,branch:1
            if (nodeDisk.isLeaf == 0)
            {
                node.IsLeaf = true;

                // put the property to the leaf
                for (int i = 0; i < nodeDisk.capacity; i++)
                {
                    node.Property.Add(new RIDKey<TK>(default(RID), nodeDisk.keyList[i]));
                }
            }
            else
            {
                node.IsLeaf = false;
                if (ridlist != null)
                {
                    foreach (var v in nodeDisk.childRidList)
                    {
                        ridlist.Add(v);
                    }
                }
            }
            if (nodeDisk.keyList != null)
            {
                foreach (var v in nodeDisk.keyList)
                    node.Values.Add(v);
            }

            return node;
        }

        public static NodeDisk<TK> ConvertNodeToNodeDisk(Node<TK, RIDKey<TK>> node)
        {
            NodeDisk<TK> nl = new NodeDisk<TK>();
            // leaf:0,branch:1
            nl.isLeaf = node.IsLeaf == true ? 0 : 1;
            nl.height = node.Height;

            if (node.Values == null) throw new Exception();

            nl.capacity = node.Values.Count;
            nl.keyList = node.Values.ToList();

            // non-leaf node
            if (node.Property == null || node.Property.Count ==0)
            {
                nl.childRidList = new List<RID>();
                foreach (var v in node.ChildrenNodes)
                {
                    if (v.CurrentRID != null && v.CurrentRID.Rid.CompareTo(new RID(-1, -1)) != 0)
                    {
                        nl.childRidList.Add(v.CurrentRID.Rid);
                    }

                }
            }

            nl.length = IndexManagerUtil<TK>.GetNodeDiskLength(nl);

            return nl;
        }
    }
}
