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

        public SM_Manager(IX_Manager<TK> ixm, RM_Manager rmm)
        {
            this.ixm = ixm;
            this.rmm = rmm;
        }

        public void OpenDb(char[] dbName)
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
            char[] relName,
            int attrCount,
            IList<Const.ConstProperty.AttrType> attributes
            )
        {
            throw new NotImplementedException();
        }

        public RID GetRelFromCat(char[] relName,
                             DataRelInfo rel)
        {
            throw new NotImplementedException();
        }

        public RID GetAttrFromCat(char[] relName,
                              char[] attrName,
                              DataAttrInfo attr)
        {
            throw new NotImplementedException();
        }

        public void DropTable(char[] relName)
        {
            throw new NotImplementedException();
        }
        #endregion

        #region index
        #endregion
    }
}
