using Database.Const;
using Database.FileManage;
using Database.IndexManage;
using Database.QueryManage;
using Database.RecordManage;
using Database.SQLOperation.data;
using Database.SystemManage;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabaseUnitTest.QLManageTest
{
    [TestClass]
    public class QLManageTest
    {
        private Func<int, string> ConverIntToString = p => { string str = Convert.ToString(p); return str; };
        private Func<string, int> ConverStringToInt = p => { int num = 0; Int32.TryParse(p, out num); return num; };
        private Func<int> CreatNewTK = () => { return 0; };
        private Func<int, int> OccupiedNum = p => { return 4; };
        string filePath = @"D:\test.txt";
        string indexPath = @"D:\IndexFile.txt";
        string metaTablePath = @"D:\meatTable.txt";
        [TestMethod]
        public void InsertRecord()
        {
            CreateQLManage();

            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);

            var fhm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(fhm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            SM_Manager<int> smm = new SM_Manager<int>(ixm, rmm, ConverStringToInt, OccupiedNum);
            QL_Manager<int> ql = new QL_Manager<int>(smm, ixm, rmm, ConverIntToString, OccupiedNum, ConverStringToInt);

            //ql.Insert();
            throw new NotImplementedException();
        }

        [TestMethod]
        public void CreateTable()
        {
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);

            var fhm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(fhm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);

            SM_Manager<int> smm = new SM_Manager<int>(ixm, rmm, ConverStringToInt, OccupiedNum);

            DataAttrInfo[] dataAttrInfos = CreateDataAttrInfo(4);

            smm.CreateTable(metaTablePath,4, dataAttrInfos.ToList());
        }

        private DataAttrInfo[] CreateDataAttrInfo(int length)
        {
            throw new NotImplementedException();
        }

        private void CreateQLManage()
        {
            // input each num to test
            int numChar = 10;
            string filePath = @"D:\test.txt";
            var fh_m = new PF_Manager();
            RM_Manager rmm = new RM_Manager(fh_m);
            rmm.CreateFile(filePath, numChar, "Record file data, use for metadata".ToArray());

            var fhm = new PF_Manager();
            IX_Manager<int> ixm = new IX_Manager<int>(fhm, ConverIntToString,
                ConverStringToInt, CreatNewTK, OccupiedNum);
            ixm.CreateFile(@"D:\IndexFile.txt", ConstProperty.AttrType.INT, 6);
        }

        [TestMethod]
        public void CreateTable()
        {
        }
    }
}
