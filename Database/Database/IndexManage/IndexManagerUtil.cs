﻿using Database.Const;
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
    public static class IndexManagerUtil<TK>
    {
        public static char[] SetIndexHeaderToChar(IX_FileHdr<TK> hdr)
        {
            char[] content = new char[ConstProperty.PF_FILE_HDR_SIZE];
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.firstFree, 0);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.numPages, ConstProperty.PF_PageHdr_SIZE);

            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.extRecordSize, 2 * ConstProperty.PF_PageHdr_SIZE);

            // IndexType
            FileManagerUtil.ReplaceTheNextFree(content
                , (int)hdr.indexType, 2 * ConstProperty.PF_PageHdr_SIZE, 1);

            // Root
            char[] charArray = SetNodeDiskToChar(hdr.root);
            FileManagerUtil.ReplaceTheNextFree(content
                , charArray, 2 * ConstProperty.PF_PageHdr_SIZE + 1, charArray.Length);

            // dicCount
            FileManagerUtil.ReplaceTheNextFree(content
                , hdr.dic.Keys.Count, 2 * ConstProperty.PF_PageHdr_SIZE + 1 + charArray.Length, ConstProperty.PF_PageHdr_SIZE);

            // KandV
            char[] charArray2 = SetDicToChar(hdr.dic);
            FileManagerUtil.ReplaceTheNextFree(content
                , charArray2, 3 * ConstProperty.PF_PageHdr_SIZE + 1 + charArray.Length, charArray2.Length);

            return content;
        }

        public static interfaceFileHdr ReadFileHdr(string fileName, FileStream fs, ConstProperty.FileType fileType, Func<string, TK> ConverStringToTK)
        {
            fs.Position = 0;
            StreamReader sr = new StreamReader(fs);

            var pf_fh = new IX_FileHdr<TK>();
            FileManagerUtil.ExtractFile(sr, pf_fh);
            ExtractIndex(sr, pf_fh, ConverStringToTK);
            return pf_fh;
        }

        public static char[] SetNodeDiskToChar(NodeDisk<TK> nl)
        {
            char[] data = new char[nl.length];
            // length
            FileManagerUtil.ReplaceTheNextFree(data, nl.length, 0);
            // isLeaf
            data[ConstProperty.Int_Size] = Convert.ToChar(nl.isLeaf);
            // capacity
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, ConstProperty.Int_Size + 1);
            // height
            FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, 2 * ConstProperty.Int_Size + 1);

            // keyList
            // TODO Set TK is int
            for (int i = 0; i < nl.capacity; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(data, nl.capacity, (3 + i) * ConstProperty.Int_Size + 1);
            }
            // childRidList
            for (int i = 0; i < nl.capacity; i++)
            {
                FileManagerUtil.ReplaceTheNextFree(data, nl.capacity,
                    (3 + nl.capacity + i) * ConstProperty.Int_Size + 1, ConstProperty.RM_Page_RID_SIZE);
            }
            return data;
        }

        public static NodeDisk<TK> SetCharToNodeDisk(char[] data, int length, Func<string, TK> ConverStringToTK)
        {
            string dataStr = new string(data).Substring(0, length);
            NodeDisk<TK> node = new NodeDisk<TK>();
            node.length = Convert.ToInt32(dataStr.Substring(0, ConstProperty.Int_Size));
            node.isLeaf = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size, 1));
            node.capacity = Convert.ToInt32(dataStr.Substring(ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
            node.height = Convert.ToInt32(dataStr.Substring(2 * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));

            for (int i = 0; i < node.capacity; i++)
            {
                TK tmp = ConverStringToTK(dataStr.Substring((3 + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                // TODO
                node.keyList[i] = tmp;
            }
            for (int i = 0; i < node.capacity; i++)
            {
                // TODO
                int pageNum = Convert.ToInt32(dataStr.Substring(
                    (3 + node.capacity + i) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                int slotNum = Convert.ToInt32(dataStr.Substring(
                    (3 + node.capacity + i + 1) * ConstProperty.Int_Size + 1, ConstProperty.Int_Size));
                node.childRidList[i] = new RID(pageNum, slotNum);
            }
            return node;
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
            sr.Read(lengthChar, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(lengthChar), out length);

            sr.BaseStream.Seek(ConstProperty.PF_FILE_HDR_NumPages_SIZE, SeekOrigin.Current);
            char[] data = new char[length];
            sr.Read(data, 0, length);
            SetCharToNodeDisk(data, length, ConverStringToTK);

            // DicCount
            sr.Read(dicCount, 0, ConstProperty.PF_FILE_HDR_NumPages_SIZE);
            Int32.TryParse(new string(totalHeight), out pf_fh.dicCount);


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

        public static Dictionary<int,int> SetCharToDic(char[] data, int length)
        {
            Dictionary<int, int> dic = new Dictionary<int, int>();

            int sum = ConstProperty.IndexHeaderKey + ConstProperty.IndexHeaderValue;

            // TODO
            while(length/ sum != 0)
            {
                char[] array = new char[ConstProperty.IndexHeaderKey];
                char[] array2 = new char[ConstProperty.IndexHeaderValue];


            }

            return null;
        }
    }
}