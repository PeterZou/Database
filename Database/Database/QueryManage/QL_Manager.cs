using Database.IndexManage;
using Database.RecordManage;
using Database.SystemManage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Database.QueryManage
{
    public class QL_Manager<TK> where TK :IComparable<TK>
    {
        RM_Manager rmm;
        IX_Manager<TK> ixm;
        SM_Manager<TK> smm;

        //
        // Constructor for the QL Manager
        //
        public QL_Manager(SM_Manager<TK> smm_, IX_Manager<TK> ixm_, RM_Manager rmm_)
        {
            this.smm = smm_;
            this.ixm = ixm_;
            this.rmm = rmm_;
        }

        // Users will call - RC invalid = IsValid(); if(invalid) return invalid; 
        private bool IsValid()
        {
            bool ret = true;
            ret = ret && (smm.IsValid() == true);
            return ret ? true : false;
        }

        private bool strlt(char[] i, char[] j)
        {
            string str1 = new string(i);
            string str2 = new string(j);
            return (str1.CompareTo(str2) < 0);
        }

        private bool streq(char[] i, char[] j)
        {
            string str1 = new string(i);
            string str2 = new string(j);
            return (str1.CompareTo(str2) == 0);
        }
    }
}
